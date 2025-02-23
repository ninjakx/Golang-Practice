package main

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/exp/rand"
)

// workers which will pull request from JOB queue
// proccessor that passes request to the JOB queue
// JOB channel -> entertainment, sports, politics

const DEFAULT_CRON_TIME = 3

var wg sync.WaitGroup

type Job struct {
	JobID int
	Msg   string
}

type Queue struct {
	channel    map[string]chan Job
	mu         sync.Mutex // locks
	topics     []string
	workerList []*Worker
}

func initQueue() *Queue {
	return &Queue{
		channel: make(map[string]chan Job),
	}
}

func (queue *Queue) createChannelForTopic(topic string, channelSize int) {
	queue.channel[topic] = make(chan Job, channelSize)
	queue.topics = append(queue.topics, topic)
}

func (queue *Queue) addJob(topic string, job Job) {
	fmt.Printf("[ARRIVED] Job:%d has arrived to queue for the topic: %s\n", job.JobID, topic)
	ch, ok := queue.channel[topic]
	if !ok {
		return
	}
	select {
	case ch <- job:
		for _, worker := range queue.workerList {
			for _, top := range worker.topicAssigned {
				if top == topic {
					// if _, ok := worker.channels[topic]; ok { // channel is not closed
					select {
					case worker.channels[topic] <- ch:
						// ch <- job // extra val will come at the end/ If matches 3 times It will feed the value 4 times. Extra val will be there
						break
					default:
					}
					// }
				}
			}
		}
		// <-ch
		fmt.Printf("[ADDED] Job:%d has been added to queue for the topic: %s\n", job.JobID, topic)
	default:
		fmt.Printf("[NOT ADDED] can't add job to the full channel for the topic: %s\n", topic)
	}
}

func (queue *Queue) shutdown() {
	for _, worker := range queue.workerList {
		worker.stopJobs()
	}
	// for _, topic := range queue.topics {
	// 	close(queue.channel[topic])
	// }
}

type Worker struct {
	workerID      int
	topicAssigned []string
	cronTime      time.Duration // at every x second do the job
	stopJob       chan bool
	mu            sync.Mutex //
	queue         *Queue
	channels      map[string]chan chan Job
}

func (queue *Queue) addWorker(worker *Worker) {
	queue.workerList = append(queue.workerList, worker)
}

func initWorker(workerID int, queue *Queue) *Worker {
	fmt.Printf("Worker:%d has been created\n", workerID)
	wok := &Worker{workerID: workerID, queue: queue, cronTime: DEFAULT_CRON_TIME * time.Second, stopJob: make(chan bool, 1), channels: make(map[string]chan chan Job)}
	// queue.addWorker(wok)
	return wok
}

func (worker *Worker) addCronTime(time time.Duration) {
	worker.cronTime = time
}

func (worker *Worker) assignedChannel(topics []string, workerChannelSize int) {
	for _, topic := range topics {
		worker.channels[topic] = make(chan chan Job, workerChannelSize) // worker local queue size
	}
}

func (worker *Worker) workerAssignedTopic(topics []string, workerChannelSize int) {
	worker.assignedChannel(topics, workerChannelSize)
	worker.topicAssigned = append(worker.topicAssigned, topics...)
}

func (worker *Worker) getWorkerAssignedTopic() []string {
	return worker.topicAssigned
}

func (worker *Worker) takeJob(topic string) {
	// queue.mu.Lock()
	// defer queue.mu.Unlock()
	job, ok := <-worker.channels[topic]
	if !ok {
		fmt.Printf("Channel is closed for topic:%s\n", topic)
		// worker.stopJobs()
		return // closed channel
	}
	jobVal := <-job
	fmt.Printf("Worker:%d is working on this job: %d for topic: %s\n", worker.workerID, jobVal.JobID, topic)
}

func (worker *Worker) startJob(queue *Queue) {
	ticker := time.NewTicker(worker.cronTime)
	defer ticker.Stop()
	defer wg.Done()
	for {
		select {
		case <-ticker.C:
			topics := worker.getWorkerAssignedTopic()
			for _, topic := range topics {
				// worker.channels[topic] <- worker.queue.channel[topic]
				worker.takeJob(topic)
			}
		case <-worker.stopJob:
			fmt.Printf("stopped jobs for worker : %d\n", worker.workerID)
			return
		}
	}
}

func (worker *Worker) stopJobs() {
	go func() {
		fmt.Printf("stopping jobs for worker : %d\n", worker.workerID)
		for _, topic := range worker.topicAssigned {
			// Other worker can close the channel so need to close if not closed already
			if _, ok := worker.channels[topic]; ok {
				close(worker.channels[topic])
			}
		}
		worker.stopJob <- true
		close(worker.stopJob)
	}()
}

func main() {
	POLITICS := "Politics"
	SPORTS := "Sports"
	ENTERTAINMENT := "Entertainment"
	topics := []string{POLITICS, SPORTS, ENTERTAINMENT}
	queue := initQueue()
	queue.createChannelForTopic(POLITICS, 15)
	queue.createChannelForTopic(SPORTS, 5)
	queue.createChannelForTopic(ENTERTAINMENT, 10)

	w1 := initWorker(1, queue)
	w2 := initWorker(2, queue)
	w3 := initWorker(3, queue)
	w1.addCronTime(1 * time.Second)
	w2.addCronTime(2 * time.Second)

	// Works fine when 1:1 mapping each worker is assigned only one topic
	w1.workerAssignedTopic([]string{SPORTS}, 100)
	w2.workerAssignedTopic([]string{POLITICS}, 50)
	w3.workerAssignedTopic([]string{ENTERTAINMENT}, 20)
	queue.addWorker(w1)
	queue.addWorker(w2)
	queue.addWorker(w3)

	go func() {
		for i := 1; i < 10; i++ {
			n := rand.Intn(10) % 3
			queue.addJob(topics[n], Job{JobID: i, Msg: fmt.Sprintf("Msg:%d", i)})
		}
	}()

	wg.Add(3)
	go w1.startJob(queue)
	go w2.startJob(queue)
	go w3.startJob(queue)

	// ======= SHUTDOWN OPERATION =======
	time.Sleep(10 * time.Second)
	// After 10 seconds shutdown the queue
	// queue.shutdown()
	w1.stopJobs()
	w2.stopJobs()
	w3.stopJobs()

	wg.Wait()
}
