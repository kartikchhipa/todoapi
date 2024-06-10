package main

import (
	"fmt"
	"strconv"
	"time"

	"strings"

	"github.com/gocql/gocql"
	"github.com/gofiber/fiber/v2"

	"github.com/go-playground/validator/v10"
)

type (
	Todo struct {
		ID          gocql.UUID `json:"id"`
		User_ID     int        `json:"user_id"`
		Title       string     `json:"title"`
		Description string     `json:"description"`
		Status      string     `json:"status"`
		Created     time.Time  `json:"created"`
		Updated     time.Time  `json:"updated"`
	}

	TodoInsert struct {
		User_ID     int    `json:"user_id" validate:"required"`
		Title       string `json:"title" validate:"required"`
		Description string `json:"description" validate:"required"`
	}

	TodoUpdate struct {
		ID          gocql.UUID `json:"id" validate:"required"`
		User_ID     int        `json:"user_id" validate:"required"`
		Title       string     `json:"title" validate:"required"`
		Description string     `json:"description" validate:"required"`
		Status      string     `json:"status" validate:"required,oneof=Pending Completed Failed"`
	}

	ErrorResponse struct {
		Error       bool
		FailedField string
		Tag         string
		Value       interface{}
	}

	XValidator struct {
		validator *validator.Validate
	}

	GlobalErrorHandlerResp struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
)

var validate = validator.New()

func (v XValidator) Validate(data interface{}) []ErrorResponse {
	validationErrors := []ErrorResponse{}

	errs := validate.Struct(data)
	if errs != nil {
		for _, err := range errs.(validator.ValidationErrors) {
			// In this case data object is actually holding the User struct
			var elem ErrorResponse

			elem.FailedField = err.Field() // Export struct field name
			elem.Tag = err.Tag()           // Export struct tag
			elem.Value = err.Value()       // Export field value
			elem.Error = true

			validationErrors = append(validationErrors, elem)
		}
	}

	return validationErrors
}

var session *gocql.Session
var err error

func main() {
	var cluster = gocql.NewCluster("node-0.gce-asia-south-2.37f994d139eecc0189cc.clusters.scylla.cloud", "node-1.gce-asia-south-2.37f994d139eecc0189cc.clusters.scylla.cloud", "node-2.gce-asia-south-2.37f994d139eecc0189cc.clusters.scylla.cloud")
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: "scylla", Password: "kartikc123123"}
	cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy("GCE_ASIA_SOUTH_2")

	session, err = cluster.CreateSession()

	if err != nil {
		panic("Failed to connect to cluster")
	}

	err = session.Query("CREATE KEYSPACE IF NOT EXISTS todo_db WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}  AND durable_writes = true;", nil).Exec()

	if err != nil {
		panic("Failed to create keyspace")
	}

	err = session.Query("CREATE TABLE IF NOT EXISTS todo_db.todos (id UUID PRIMARY KEY, user_id int, title text, description text, status text, created timestamp, updated timestamp);", nil).Exec()

	if err != nil {
		fmt.Println(err)
		panic("Failed to create table")
	}

	defer session.Close()

	myValidator := &XValidator{
		validator: validate,
	}

	app := fiber.New(fiber.Config{
		// Global custom error handler
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			return c.Status(fiber.StatusBadRequest).JSON(GlobalErrorHandlerResp{
				Success: false,
				Message: err.Error(),
			})
		},
	})

	// create dummy data

	for i := 0; i < 10; i++ {
		todo := new(Todo)
		todo.ID = gocql.TimeUUID()
		timeNow := time.Now()
		todo.Created = timeNow
		todo.Updated = timeNow
		todo.User_ID = i
		todo.Title = fmt.Sprintf("Title %d", i)
		todo.Description = fmt.Sprintf("Description %d", i)
		todo.Status = "Pending"

		err := session.Query("INSERT INTO todo_db.todos (id, user_id, title, description, status, created, updated) VALUES (?, ?, ?, ?, ?, ?, ?);", todo.ID, todo.User_ID, todo.Title, todo.Description, todo.Status, todo.Created, todo.Updated).Exec()

		if err != nil {
			fmt.Println(err)
			panic("Failed to insert")
		}
	}

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello, World!")
	})

	// CRUD Operations

	// Insert

	app.Post("/insert/", func(c *fiber.Ctx) error {

		c.Accepts("application/json")

		todoInsert := new(TodoInsert)
		todo := new(Todo)

		if err := c.BodyParser(todoInsert); err != nil {
			return c.Status(400).SendString("Failed to parse JSON")
		}

		if errs := myValidator.Validate(todoInsert); len(errs) > 0 {
			errMsgs := make([]string, 0)

			for _, err := range errs {
				errMsgs = append(errMsgs, fmt.Sprintf(
					"[%s]: '%v' | Needs to implement '%s'",
					err.FailedField,
					err.Value,
					err.Tag,
				))
			}

			return &fiber.Error{
				Code:    fiber.ErrBadRequest.Code,
				Message: strings.Join(errMsgs, " and "),
			}
		}

		todo.ID = gocql.TimeUUID()
		timeNow := time.Now()
		todo.Created = timeNow
		todo.Updated = timeNow
		todo.User_ID = todoInsert.User_ID
		todo.Title = todoInsert.Title
		todo.Description = todoInsert.Description
		todo.Status = "Pending"

		err := session.Query("INSERT INTO todo_db.todos (id, user_id, title, description, status, created, updated) VALUES (?, ?, ?, ?, ?, ?, ?);", todo.ID, todo.User_ID, todo.Title, todo.Description, todo.Status, todo.Created, todo.Updated).Exec()

		if err != nil {
			return c.Status(500).SendString("Failed to insert")
		}

		return c.JSON(todo)

	})

	// Delete by ID

	app.Delete("/delete", func(c *fiber.Ctx) error {

		id := c.Query("id")

		if id == "" {
			return c.Status(400).SendString("ID is required")
		}

		uuid, err := gocql.ParseUUID(id)

		if err != nil {
			return c.Status(400).SendString("Invalid ID")
		}

		err = session.Query("DELETE FROM todo_db.todos WHERE id = ?;", uuid).Exec()

		if err != nil {
			return c.Status(500).SendString("Failed to delete")
		}

		return c.SendString("Deleted")

	})

	var maxPageSize int = 100
	var defaultPageSize int = 10

	// Pagination with limit and page

	app.Get("/get", func(c *fiber.Ctx) error {

		resultsPerPage := c.Query("limit")
		page := c.Query("page")

		if resultsPerPage == "" || page == "" {
			return c.Status(400).SendString("Limit and Page are required")
		}

		resultsPerPageInt, err := strconv.Atoi(resultsPerPage)

		fmt.Println(resultsPerPageInt)

		if err != nil {
			return c.Status(400).SendString("Invalid limit")
		}

		if resultsPerPageInt > maxPageSize {
			resultsPerPageInt = maxPageSize
		}

		if resultsPerPageInt < defaultPageSize {
			resultsPerPageInt = defaultPageSize
		}

		pageInt, err := strconv.Atoi(page)

		if err != nil {
			return c.Status(400).SendString("Invalid page")
		}

		if pageInt < 0 {
			pageInt = 0
		}

		var pageState []byte

		for i := 0; i < pageInt; i++ {

			iter := session.Query("SELECT id, user_id, title, description, status, created, updated FROM todo_db.todos;").PageSize(resultsPerPageInt).PageState(pageState).Iter()
			nextPageState := iter.PageState()

			if iter.NumRows() == 0 {
				return c.JSON([]Todo{})
			}

			if iter.NumRows() < resultsPerPageInt {

				var todos []Todo
				scanner := iter.Scanner()
				for scanner.Next() {
					todo := new(Todo)
					err := scanner.Scan(&todo.ID, &todo.User_ID, &todo.Title, &todo.Description, &todo.Status, &todo.Created, &todo.Updated)
					if err != nil {
						return c.Status(500).SendString("Failed to scan")
					}
					todos = append(todos, *todo)
				}

				return c.JSON(todos)

			}

			if i == pageInt-1 {

				var todos []Todo
				scanner := iter.Scanner()
				for scanner.Next() {
					todo := new(Todo)
					err := scanner.Scan(&todo.ID, &todo.User_ID, &todo.Title, &todo.Description, &todo.Status, &todo.Created, &todo.Updated)
					if err != nil {
						return c.Status(500).SendString("Failed to scan")
					}
					todos = append(todos, *todo)
				}

				return c.JSON(todos)

			}

			if nextPageState == nil {
				return c.JSON([]Todo{})
			}

			pageState = nextPageState
		}

		return c.SendString("Got")

	})

	// Update

	app.Put("/update", func(c *fiber.Ctx) error {

		todoUpdate := new(TodoUpdate)
		c.Accepts("application/json")

		if err := c.BodyParser(todoUpdate); err != nil {
			return c.Status(400).SendString("Failed to parse JSON")
		}

		if errs := myValidator.Validate(todoUpdate); len(errs) > 0 {
			errMsgs := make([]string, 0)

			for _, err := range errs {
				errMsgs = append(errMsgs, fmt.Sprintf(
					"[%s]: '%v' | Needs to implement '%s'",
					err.FailedField,
					err.Value,
					err.Tag,
				))
			}

			return &fiber.Error{
				Code:    fiber.ErrBadRequest.Code,
				Message: strings.Join(errMsgs, " and "),
			}
		}

		query := session.Query("UPDATE todo_db.todos SET user_id = ?, title = ?, description = ?, status = ?, updated = ? WHERE id = ?;", todoUpdate.User_ID, todoUpdate.Title, todoUpdate.Description, todoUpdate.Status, time.Now(), todoUpdate.ID)

		err := query.Exec()

		if err != nil {
			fmt.Println(err)
			return c.Status(500).SendString("Failed to update")
		}

		return c.Status(200).SendString("Updated")

	})

	app.Listen(":3000")

}
