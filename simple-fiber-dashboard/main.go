package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AsynqLab/asynq"
	"github.com/AsynqLab/asynqmon"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
)

// Task types
const (
	TypeEmailDelivery = "email:deliver"
)

// createEmailDeliveryTask creates a new task for email delivery.
func createEmailDeliveryTask(email string) *asynq.Task {
	// Task Payload as JSON
	payload := map[string]interface{}{"email": email}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return asynq.NewTask(TypeEmailDelivery, payloadBytes)
}

// handleEmailDeliveryTask handles the email delivery task.
func handleEmailDeliveryTask(_ context.Context, t *asynq.Task) error {
	// Extract payload
	var payload map[string]interface{}
	err := json.Unmarshal(t.Payload(), &payload)
	if err != nil {
		return err
	}
	email := payload["email"]

	// Simulate sending email
	log.Printf("Sending email to %s...", email)
	time.Sleep(2 * time.Second) // Simulate delay
	log.Printf("Email successfully sent to %s", email)

	return nil
}

func main() {
	redisConnection := asynq.RedisClientOpt{Addr: "localhost:6379"}

	client := asynq.NewClient(redisConnection)
	defer client.Close()

	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		email := c.Query("email")
		if email == "" {
			return c.SendString("Email is required")
		}

		task := createEmailDeliveryTask(email)
		taskInfo, err := client.Enqueue(task, asynq.ProcessIn(2*time.Second), asynq.Retention(5*24*time.Hour))
		if err != nil {
			log.Fatal("Failed to enqueue task:", err)
		}
		fmt.Printf("Task %s enqueued with ID %s\n", task.Type(), taskInfo.ID)

		return c.SendString("Hello, World!")
	})

	// In the main() function, add this code after creating the Fiber app
	inspector := asynq.NewInspector(redisConnection)

	app.Get("/task/:id", func(c *fiber.Ctx) error {
		taskID := c.Params("id")
		if taskID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Task ID is required",
			})
		}

		taskInfo, err := getTaskInfo(inspector, taskID)
		if err != nil {
			if err == asynq.ErrTaskNotFound {
				return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
					"error": "Task not found",
				})
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve task information",
			})
		}

		return c.JSON(fiber.Map{
			"id":              taskInfo.ID,
			"type":            taskInfo.Type,
			"payload":         string(taskInfo.Payload),
			"queue":           taskInfo.Queue,
			"max_retry":       taskInfo.MaxRetry,
			"retention":       taskInfo.Retention.String(),
			"last_failed_at":  taskInfo.LastFailedAt,
			"is_orphaned":     taskInfo.IsOrphaned,
			"next_process_at": taskInfo.NextProcessAt,
			"deadline":        taskInfo.Deadline,
			"completed_at":    taskInfo.CompletedAt,
			"retry_count":     taskInfo.Retried,
		})
	})

	monitoring := asynqmon.New(asynqmon.Options{
		RootPath:     "/monitoring", // RootPath specifies the root for asynqmon app
		RedisConnOpt: redisConnection,
	})
	app.All(fmt.Sprintf("%s/*", monitoring.RootPath()), adaptor.HTTPHandler(monitoring))

	// Task processing server
	asynqServer := asynq.NewServer(redisConnection, asynq.Config{Concurrency: 10})

	// Define task handlers
	mux := asynq.NewServeMux()
	mux.HandleFunc(TypeEmailDelivery, handleEmailDeliveryTask) // Handle email delivery task

	app.Hooks().OnListen(func(listenData fiber.ListenData) error {
		if fiber.IsChild() {
			return nil
		}
		go func() {
			if err := asynqServer.Run(mux); err != nil {
				panic(err)
			}
		}()
		return nil
	})

	go func() {
		if err := app.Listen(":3000"); err != nil {
			log.Fatal("Failed to start server:", err)
		}
	}()

	quitCh := initQuitCh()
	sig := <-quitCh // This blocks the main thread until an interrupt is received
	log.Println("Received signal:", sig)

	asynqServer.Shutdown()
}

func initQuitCh() chan os.Signal {
	sigCh := make(chan os.Signal, 1) // Create channel to signify a signal being sent
	signal.Notify(
		sigCh,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	) // When an interrupt or termination signal is sent, notify the channel

	return sigCh
}

func getTaskInfo(inspector *asynq.Inspector, taskID string) (*asynq.TaskInfo, error) {
	return inspector.GetTaskInfo("default", taskID)
}
