package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	timeAddition       = 1000
	timeSubtraction    = 1000
	timeMultiplication = 2000
	timeDivision       = 2000
)

func init() {
	if val, exists := os.LookupEnv("TIME_ADDITION_MS"); exists {
		if v, err := strconv.Atoi(val); err == nil {
			timeAddition = v
		}
	}
	if val, exists := os.LookupEnv("TIME_SUBTRACTION_MS"); exists {
		if v, err := strconv.Atoi(val); err == nil {
			timeSubtraction = v
		}
	}
	if val, exists := os.LookupEnv("TIME_MULTIPLICATIONS_MS"); exists {
		if v, err := strconv.Atoi(val); err == nil {
			timeMultiplication = v
		}
	}
	if val, exists := os.LookupEnv("TIME_DIVISIONS_MS"); exists {
		if v, err := strconv.Atoi(val); err == nil {
			timeDivision = v
		}
	}
}

type Task struct {
	ID            string      `json:"id"`
	ExpressionID  string      `json:"expression_id"`
	Arg1          interface{} `json:"arg1"`
	Arg2          interface{} `json:"arg2"`
	Operation     string      `json:"operation"`
	OperationTime int         `json:"operation_time"`
	Status        string      `json:"status"`
	Result        float64     `json:"result"`
}

type Expression struct {
	ID         string    `json:"id"`
	Expression string    `json:"expression"`
	Status     string    `json:"status"`
	Result     float64   `json:"result"`
	CreatedAt  time.Time `json:"created_at"`
	Tasks      []*Task   `json:"-"`
}

type Storage struct {
	sync.RWMutex
	Expressions    map[string]*Expression
	Tasks          map[string]*Task
	PendingTasks   []string
	TaskDependents map[string][]string
}

func NewStorage() *Storage {
	return &Storage{
		Expressions:    make(map[string]*Expression),
		Tasks:          make(map[string]*Task),
		PendingTasks:   make([]string, 0),
		TaskDependents: make(map[string][]string),
	}
}

type Orchestrator struct {
	storage *Storage
}

func NewOrchestrator() *Orchestrator {
	return &Orchestrator{
		storage: NewStorage(),
	}
}

func (o *Orchestrator) Start(port string) {
	http.HandleFunc("/api/v1/calculate", o.handleCalculate)
	http.HandleFunc("/api/v1/expressions", o.handleGetExpressions)
	http.HandleFunc("/api/v1/expressions/", o.handleGetExpression)
	http.HandleFunc("/internal/task", o.handleTask)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func (o *Orchestrator) handleCalculate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Expression string `json:"expression"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusUnprocessableEntity)
		return
	}

	expr := &Expression{
		ID:         generateID(),
		Expression: req.Expression,
		Status:     "pending",
		CreatedAt:  time.Now(),
	}

	tasks, err := parseExpression(expr.ID, req.Expression)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing expression: %v", err), http.StatusUnprocessableEntity)
		return
	}

	expr.Tasks = tasks

	o.storage.Lock()
	o.storage.Expressions[expr.ID] = expr
	for _, task := range tasks {
		o.storage.Tasks[task.ID] = task
		if isReady(task) {
			o.storage.PendingTasks = append(o.storage.PendingTasks, task.ID)
		}
		if arg, ok := task.Arg1.(string); ok {
			o.storage.TaskDependents[arg] = append(o.storage.TaskDependents[arg], task.ID)
		}
		if arg, ok := task.Arg2.(string); ok {
			o.storage.TaskDependents[arg] = append(o.storage.TaskDependents[arg], task.ID)
		}
	}
	o.storage.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"id": expr.ID})
}

func (o *Orchestrator) handleGetExpressions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	o.storage.RLock()
	defer o.storage.RUnlock()

	expressions := make([]map[string]interface{}, 0, len(o.storage.Expressions))
	for _, expr := range o.storage.Expressions {
		expressions = append(expressions, map[string]interface{}{
			"id":         expr.ID,
			"status":     expr.Status,
			"result":     expr.Result,
			"expression": expr.Expression,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"expressions": expressions})
}

func (o *Orchestrator) handleGetExpression(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pathParts := strings.Split(r.URL.Path, "/")
	id := pathParts[4]

	o.storage.RLock()
	expr, exists := o.storage.Expressions[id]
	o.storage.RUnlock()

	if !exists {
		http.Error(w, "Expression not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"expression": map[string]interface{}{
			"id":         expr.ID,
			"status":     expr.Status,
			"result":     expr.Result,
			"expression": expr.Expression,
		},
	})
}

func (o *Orchestrator) handleTask(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		o.handleGetTask(w, r)
	case http.MethodPost:
		o.handlePostTaskResult(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (o *Orchestrator) handleGetTask(w http.ResponseWriter, r *http.Request) {
	o.storage.Lock()
	defer o.storage.Unlock()

	if len(o.storage.PendingTasks) == 0 {
		http.Error(w, "No tasks available", http.StatusNotFound)
		return
	}

	taskID := o.storage.PendingTasks[0]
	o.storage.PendingTasks = o.storage.PendingTasks[1:]

	task := o.storage.Tasks[taskID]
	task.Status = "processing"

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"task": map[string]interface{}{
			"id":             task.ID,
			"arg1":          task.Arg1,
			"arg2":          task.Arg2,
			"operation":     task.Operation,
			"operation_time": task.OperationTime,
		},
	})
}

func (o *Orchestrator) handlePostTaskResult(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     string  `json:"id"`
		Result float64 `json:"result"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusUnprocessableEntity)
		return
	}

	o.storage.Lock()
	defer o.storage.Unlock()

	task, exists := o.storage.Tasks[req.ID]
	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	task.Status = "done"
	task.Result = req.Result

	for _, dependentID := range o.storage.TaskDependents[task.ID] {
		dependentTask := o.storage.Tasks[dependentID]
		if isReady(dependentTask) {
			o.storage.PendingTasks = append(o.storage.PendingTasks, dependentID)
		}
	}

	expr := o.storage.Expressions[task.ExpressionID]
	allDone := true
	for _, t := range expr.Tasks {
		if t.Status != "done" {
			allDone = false
			break
		}
	}

	if allDone {
		var finalTask *Task
		for _, t := range expr.Tasks {
			if len(o.storage.TaskDependents[t.ID]) == 0 {
				finalTask = t
				break
			}
		}

		if finalTask != nil {
			expr.Status = "done"
			expr.Result = finalTask.Result
		}
	}

	w.WriteHeader(http.StatusOK)
}

type Agent struct {
	orchestratorURL string
	computingPower  int
}

func NewAgent(orchestratorURL string, computingPower int) *Agent {
	return &Agent{
		orchestratorURL: orchestratorURL,
		computingPower:  computingPower,
	}
}

func (a *Agent) Start() {
	var wg sync.WaitGroup
	for i := 0; i < a.computingPower; i++ {
		wg.Add(1)
		go a.worker(i+1, &wg)
	}
	wg.Wait()
}

func (a *Agent) worker(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		task, err := a.getTask()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		if task == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		result, err := a.computeTask(task)
		if err != nil {
			continue
		}

		if err := a.sendResult(task.ID, result); err != nil {
			continue
		}
	}
}

func (a *Agent) getTask() (*Task, error) {
	resp, err := http.Get(a.orchestratorURL + "/internal/task")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response struct {
		Task struct {
			ID            string      `json:"id"`
			Arg1          interface{} `json:"arg1"`
			Arg2          interface{} `json:"arg2"`
			Operation     string      `json:"operation"`
			OperationTime int         `json:"operation_time"`
		} `json:"task"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}

	return &Task{
		ID:            response.Task.ID,
		Arg1:          response.Task.Arg1,
		Arg2:          response.Task.Arg2,
		Operation:     response.Task.Operation,
		OperationTime: response.Task.OperationTime,
	}, nil
}

func (a *Agent) computeTask(task *Task) (float64, error) {
	time.Sleep(time.Duration(task.OperationTime) * time.Millisecond)

	arg1, err := getArgValue(task.Arg1)
	if err != nil {
		return 0, err
	}

	arg2, err := getArgValue(task.Arg2)
	if err != nil {
		return 0, err
	}

	switch task.Operation {
	case "+":
		return arg1 + arg2, nil
	case "-":
		return arg1 - arg2, nil
	case "*":
		return arg1 * arg2, nil
	case "/":
		if arg2 == 0 {
			return 0, errors.New("division by zero")
		}
		return arg1 / arg2, nil
	default:
		return 0, fmt.Errorf("unknown operation: %s", task.Operation)
	}
}

func (a *Agent) sendResult(taskID string, result float64) error {
	data := struct {
		ID     string  `json:"id"`
		Result float64 `json:"result"`
	}{
		ID:     taskID,
		Result: result,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	resp, err := http.Post(a.orchestratorURL+"/internal/task", "application/json", strings.NewReader(string(jsonData)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func parseExpression(exprID, expr string) ([]*Task, error) {
	rpn, err := shuntingYard(expr)
	if err != nil {
		return nil, err
	}

	stack := []interface{}{}
	tasks := []*Task{}

	for _, token := range rpn {
		if num, err := strconv.ParseFloat(token, 64); err == nil {
			stack = append(stack, num)
		} else {
			if len(stack) < 2 {
				return nil, errors.New("invalid expression: not enough operands")
			}

			arg2 := stack[len(stack)-1]
			arg1 := stack[len(stack)-2]
			stack = stack[:len(stack)-2]

			task := &Task{
				ID:            generateID(),
				ExpressionID:  exprID,
				Arg1:          arg1,
				Arg2:          arg2,
				Operation:     token,
				OperationTime: getOperationTime(token),
				Status:        "pending",
			}

			tasks = append(tasks, task)
			stack = append(stack, task.ID)
		}
	}

	if len(stack) != 1 {
		return nil, errors.New("invalid expression: could not reduce to single result")
	}

	return tasks, nil
}

func shuntingYard(expr string) ([]string, error) {
	var output []string
	var operators []string

	precedence := func(op string) int {
		switch op {
		case "+", "-":
			return 1
		case "*", "/":
			return 2
		}
		return 0
	}

	i := 0
	for i < len(expr) {
		c := expr[i]

		switch {
		case c == ' ':
			i++
			continue
		case c >= '0' && c <= '9' || c == '.':
			j := i
			for j < len(expr) && (expr[j] >= '0' && expr[j] <= '9' || expr[j] == '.') {
				j++
			}
			output = append(output, expr[i:j])
			i = j
		case c == '(':
			operators = append(operators, string(c))
			i++
		case c == ')':
			for len(operators) > 0 && operators[len(operators)-1] != "(" {
				output = append(output, operators[len(operators)-1])
				operators = operators[:len(operators)-1]
			}
			if len(operators) == 0 {
				return nil, errors.New("mismatched parentheses")
			}
			operators = operators[:len(operators)-1]
			i++
		default:
			op := string(c)
			for len(operators) > 0 && precedence(operators[len(operators)-1]) >= precedence(op) {
				output = append(output, operators[len(operators)-1])
				operators = operators[:len(operators)-1]
			}
			operators = append(operators, op)
			i++
		}
	}

	for len(operators) > 0 {
		if operators[len(operators)-1] == "(" {
			return nil, errors.New("mismatched parentheses")
		}
		output = append(output, operators[len(operators)-1])
		operators = operators[:len(operators)-1]
	}

	return output, nil
}

func getOperationTime(op string) int {
	switch op {
	case "+":
		return timeAddition
	case "-":
		return timeSubtraction
	case "*":
		return timeMultiplication
	case "/":
		return timeDivision
	default:
		return 1000
	}
}

func isReady(task *Task) bool {
	if _, ok := task.Arg1.(string); ok {
		return false
	}
	if _, ok := task.Arg2.(string); ok {
		return false
	}
	return true
}

func getArgValue(arg interface{}) (float64, error) {
	switch v := arg.(type) {
	case float64:
		return v, nil
	case string:
		return 0, errors.New("task result not available")
	default:
		return 0, fmt.Errorf("invalid arg type: %T", arg)
	}
}

func main() {
	orchestrator := NewOrchestrator()
	go orchestrator.Start("8080")

	computingPower := 4
	if val, exists := os.LookupEnv("COMPUTING_POWER"); exists {
		if v, err := strconv.Atoi(val); err == nil {
			computingPower = v
		}
	}

	agent := NewAgent("http://localhost:8080", computingPower)
	agent.Start()
}

func TestOrchestratorEndpoints(t *testing.T) {
	o := NewOrchestrator()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/calculate":
			o.handleCalculate(w, r)
		case r.URL.Path == "/api/v1/expressions":
			o.handleGetExpressions(w, r)
		case strings.HasPrefix(r.URL.Path, "/api/v1/expressions/"):
			o.handleGetExpression(w, r)
		case r.URL.Path == "/internal/task":
			if r.Method == http.MethodGet {
				o.handleGetTask(w, r)
			} else {
				o.handlePostTaskResult(w, r)
			}
		}
	}))
	defer ts.Close()

	t.Run("TestCalculateHandler", func(t *testing.T) {
		expr := `{"expression": "2 + 2 * 2"}`
		resp, err := http.Post(ts.URL+"/api/v1/calculate", "application/json", bytes.NewBufferString(expr))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status 201, got %d", resp.StatusCode)
		}

		var result struct{ ID string }
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if result.ID == "" {
			t.Error("Expected non-empty ID")
		}
	})

	t.Run("TestTaskProcessing", func(t *testing.T) {
		// Add simple expression
		expr := `{"expression": "3 + 4"}`
		resp, _ := http.Post(ts.URL+"/api/v1/calculate", "application/json", bytes.NewBufferString(expr))
		var exprResult struct{ ID string }
		json.NewDecoder(resp.Body).Decode(&exprResult)
		resp.Body.Close()

		// Get task
		resp, err := http.Get(ts.URL + "/internal/task")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		var taskResp struct {
			Task struct {
				ID     string      `json:"id"`
				Arg1   interface{} `json:"arg1"`
				Arg2   interface{} `json:"arg2"`
				Operation string   `json:"operation"`
			} `json:"task"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&taskResp); err != nil {
			t.Fatal(err)
		}

		// Submit result
		result := map[string]interface{}{"id": taskResp.Task.ID, "result": 7.0}
		jsonData, _ := json.Marshal(result)
		resp, _ = http.Post(ts.URL+"/internal/task", "application/json", bytes.NewBuffer(jsonData))
		resp.Body.Close()

		// Verify expression status
		resp, _ = http.Get(ts.URL + "/api/v1/expressions/" + exprResult.ID)
		var status struct {
			Expression struct {
				Status string  `json:"status"`
				Result float64 `json:"result"`
			} `json:"expression"`
		}
		json.NewDecoder(resp.Body).Decode(&status)
		resp.Body.Close()

		if status.Expression.Status != "done" || status.Expression.Result != 7.0 {
			t.Errorf("Expected done status with result 7, got %s/%f", 
				status.Expression.Status, status.Expression.Result)
		}
	})
}

func TestExpressionParsing(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantLen int
		wantErr bool
	}{
		{"Simple", "2 + 3", 1, false},
		{"Complex", "2 + 3 * 4", 2, false},
		{"Invalid", "2 + * 3", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks, err := parseExpression("test-id", tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseExpression() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(tasks) != tt.wantLen {
				t.Errorf("Expected %d tasks, got %d", tt.wantLen, len(tasks))
			}
		})
	}
}

func TestAgentWorkflow(t *testing.T) {
	// Mock orchestrator
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Write([]byte(`{"task":{"id":"test-task","arg1":5,"arg2":3,"operation":"+","operation_time":100}}`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	agent := NewAgent(ts.URL, 1)
	
	t.Run("TaskProcessing", func(t *testing.T) {
		task, err := agent.getTask()
		if err != nil {
			t.Fatal(err)
		}

		result, err := agent.computeTask(task)
		if err != nil || result != 8 {
			t.Errorf("Expected result 8, got %f (err: %v)", result, err)
		}

		if err := agent.sendResult(task.ID, result); err != nil {
			t.Errorf("Failed to send result: %v", err)
		}
	})
}

func TestStorageConcurrency(t *testing.T) {
	storage := NewStorage()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			task := &Task{
				ID:    "task-" + strconv.Itoa(i),
				Arg1:  float64(i),
				Arg2:  float64(i),
				Operation: "+",
			}

			storage.Lock()
			storage.Tasks[task.ID] = task
			storage.PendingTasks = append(storage.PendingTasks, task.ID)
			storage.Unlock()
		}(i)
	}

	wg.Wait()

	if len(storage.Tasks) != 100 || len(storage.PendingTasks) != 100 {
		t.Errorf("Concurrency issue: tasks=%d, pending=%d", 
			len(storage.Tasks), len(storage.PendingTasks))
	}
}

func TestMain(m *testing.M) {
	timeAddition = 0
	timeSubtraction = 0
	timeMultiplication = 0
	timeDivision = 0

	code := m.Run()

	os.Exit(code)
}
