package main

import (
	"fmt"
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

// These workers will receive work on the `jobs` channel and send the corresponding results on `results`.
func worker(id int, jobs <-chan result, results chan<- int, workers chan<- int) {
	for j := range jobs {
		fmt.Println("worker", id, "processing job", j.id, "number", j.number)
		results <- j.number
	}
	workers <- 1
}

func master(id int, jobs chan<- result, masters chan<- int) {

	// Open DB conenction
	db, err := sql.Open("mysql", "golang:nethack@tcp(127.0.0.1:3306)/go")
	if err != nil {
		log.Println(err)
	}
	defer db.Close()

	// Run a query to fill a job queue
	q := "SELECT id, number FROM go.mysql_worker where processed != 1"
	rows, err := db.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Loop over results
	for rows.Next() {
		var id int
		var number int
		if err := rows.Scan(&id, &number); err != nil {
			log.Fatal(err)
		}
		jobs <- result{id, number}
	}
	masters <- 1
}

type result struct {
	id  int
	number  int
}

func main() {

	// In order to use our pool of workers we need to send
	// them work and collect their results. We make 2
	// channels for this.
	jobs := make(chan result, 100)
	results := make(chan int, 100)

	// Monitor for completion of threads on these chann	masters := make(chan int)
	workers := make(chan int)
	masters := make(chan int)

	// Vars to hold numbers of masters and workers we want
	var workers_count int = 10
	var masters_count int = 1

	// This starts up workers and masters, initially blocked
	// because there are no jobs yet.
	// Possible race conidtion if starting all threads is slower than the time taken for one to finish
	for w := 1; w <= workers_count; w++ {
	      go worker(w, jobs, results, workers)
	}
	for i := 1; i <= masters_count; i++ {
	      go master(i, jobs, masters)
	}

	for {
		select {
			// Consume Results as they come in
			case <-results:
				// All workers and Masters have finished working, so end
				if workers_count == 0 && masters_count == 0 {
					fmt.Println("Processed all rows")
					return
				}

			// Consume Masters when they finish
			case <-masters:
				masters_count--
				if masters_count == 0 {
					close(jobs)
				}
			// Consume workers when they finish
			case <-workers:
				workers_count--
				if workers_count == 0 {
					close(results)
				}
		}
	}
}
