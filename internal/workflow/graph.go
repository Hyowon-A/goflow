package workflow

import "slices"

type DependencyEdge struct {
	PredecessorTaskID string
	SuccessorTaskID   string
}

type WorkflowGraph struct {
	TaskIDs     []string
	Outgoing    map[string][]string
	InDegree    map[string]int
	RootTaskIDs []string
	LeafTaskIDs []string
}

func NewWorkflowGraph(taskIDs []string, edges []DependencyEdge) (WorkflowGraph, error) {
	graph := WorkflowGraph{
		TaskIDs:  slices.Clone(taskIDs),
		Outgoing: make(map[string][]string, len(taskIDs)),
		InDegree: make(map[string]int, len(taskIDs)),
	}

	knownTasks := make(map[string]struct{}, len(taskIDs))
	for _, taskID := range taskIDs {
		knownTasks[taskID] = struct{}{}
		graph.Outgoing[taskID] = nil
		graph.InDegree[taskID] = 0
	}

	for _, edge := range edges {
		if _, ok := knownTasks[edge.PredecessorTaskID]; !ok {
			return WorkflowGraph{}, ErrInvalidTaskReference
		}
		if _, ok := knownTasks[edge.SuccessorTaskID]; !ok {
			return WorkflowGraph{}, ErrInvalidTaskReference
		}
		if edge.PredecessorTaskID == edge.SuccessorTaskID {
			return WorkflowGraph{}, ErrSelfDependency
		}

		graph.Outgoing[edge.PredecessorTaskID] = append(graph.Outgoing[edge.PredecessorTaskID], edge.SuccessorTaskID)
		graph.InDegree[edge.SuccessorTaskID]++
	}

	for _, taskID := range graph.TaskIDs {
		slices.Sort(graph.Outgoing[taskID])

		if graph.InDegree[taskID] == 0 {
			graph.RootTaskIDs = append(graph.RootTaskIDs, taskID)
		}
		if len(graph.Outgoing[taskID]) == 0 {
			graph.LeafTaskIDs = append(graph.LeafTaskIDs, taskID)
		}
	}

	slices.Sort(graph.RootTaskIDs)
	slices.Sort(graph.LeafTaskIDs)

	if hasDependencyCycle(graph) {
		return WorkflowGraph{}, ErrDependencyCycle
	}

	return graph, nil
}

func hasDependencyCycle(graph WorkflowGraph) bool {
	inDegree := make(map[string]int, len(graph.InDegree))
	for taskID, degree := range graph.InDegree {
		inDegree[taskID] = degree
	}

	queue := slices.Clone(graph.RootTaskIDs)
	visited := 0

	for len(queue) > 0 {
		taskID := queue[0]
		queue = queue[1:]
		visited++

		for _, successorID := range graph.Outgoing[taskID] {
			inDegree[successorID]--
			if inDegree[successorID] == 0 {
				queue = append(queue, successorID)
			}
		}
		slices.Sort(queue)
	}

	return visited < len(graph.TaskIDs)
}
