package workflow

import (
	"errors"
	"reflect"
	"testing"
)

func TestNewWorkflowGraphBuildsAdjacencyListAndInDegree(t *testing.T) {
	graph, err := NewWorkflowGraph(
		[]string{"A", "B", "C", "D"},
		[]DependencyEdge{
			{PredecessorTaskID: "A", SuccessorTaskID: "B"},
			{PredecessorTaskID: "A", SuccessorTaskID: "C"},
			{PredecessorTaskID: "B", SuccessorTaskID: "D"},
			{PredecessorTaskID: "C", SuccessorTaskID: "D"},
		},
	)
	if err != nil {
		t.Fatalf("build workflow graph: %v", err)
	}

	wantOutgoing := map[string][]string{
		"A": {"B", "C"},
		"B": {"D"},
		"C": {"D"},
		"D": nil,
	}
	if !reflect.DeepEqual(graph.Outgoing, wantOutgoing) {
		t.Fatalf("unexpected outgoing edges: got %#v, want %#v", graph.Outgoing, wantOutgoing)
	}

	wantInDegree := map[string]int{
		"A": 0,
		"B": 1,
		"C": 1,
		"D": 2,
	}
	if !reflect.DeepEqual(graph.InDegree, wantInDegree) {
		t.Fatalf("unexpected in-degree counts: got %#v, want %#v", graph.InDegree, wantInDegree)
	}

	if !reflect.DeepEqual(graph.RootTaskIDs, []string{"A"}) {
		t.Fatalf("unexpected root task IDs: got %#v", graph.RootTaskIDs)
	}

	if !reflect.DeepEqual(graph.LeafTaskIDs, []string{"D"}) {
		t.Fatalf("unexpected leaf task IDs: got %#v", graph.LeafTaskIDs)
	}
}

func TestNewWorkflowGraphSortsOutput(t *testing.T) {
	graph, err := NewWorkflowGraph(
		[]string{"C", "A", "D", "B"},
		[]DependencyEdge{
			{PredecessorTaskID: "A", SuccessorTaskID: "D"},
			{PredecessorTaskID: "C", SuccessorTaskID: "B"},
			{PredecessorTaskID: "A", SuccessorTaskID: "B"},
		},
	)
	if err != nil {
		t.Fatalf("build workflow graph: %v", err)
	}

	if !reflect.DeepEqual(graph.Outgoing["A"], []string{"B", "D"}) {
		t.Fatalf("expected sorted outgoing edges for A, got %#v", graph.Outgoing["A"])
	}

	if !reflect.DeepEqual(graph.RootTaskIDs, []string{"A", "C"}) {
		t.Fatalf("expected sorted root task IDs, got %#v", graph.RootTaskIDs)
	}

	if !reflect.DeepEqual(graph.LeafTaskIDs, []string{"B", "D"}) {
		t.Fatalf("expected sorted leaf task IDs, got %#v", graph.LeafTaskIDs)
	}
}

func TestNewWorkflowGraphRejectsInvalidTaskReference(t *testing.T) {
	_, err := NewWorkflowGraph(
		[]string{"A"},
		[]DependencyEdge{
			{PredecessorTaskID: "A", SuccessorTaskID: "B"},
		},
	)
	if !errors.Is(err, ErrInvalidTaskReference) {
		t.Fatalf("expected ErrInvalidTaskReference, got %v", err)
	}
}

func TestNewWorkflowGraphRejectsSelfDependency(t *testing.T) {
	_, err := NewWorkflowGraph(
		[]string{"A"},
		[]DependencyEdge{
			{PredecessorTaskID: "A", SuccessorTaskID: "A"},
		},
	)
	if !errors.Is(err, ErrSelfDependency) {
		t.Fatalf("expected ErrSelfDependency, got %v", err)
	}
}

func TestNewWorkflowGraphRejectsDependencyCycles(t *testing.T) {
	tests := []struct {
		name    string
		taskIDs []string
		edges   []DependencyEdge
	}{
		{
			name:    "two node cycle",
			taskIDs: []string{"A", "B"},
			edges: []DependencyEdge{
				{PredecessorTaskID: "A", SuccessorTaskID: "B"},
				{PredecessorTaskID: "B", SuccessorTaskID: "A"},
			},
		},
		{
			name:    "longer cycle",
			taskIDs: []string{"A", "B", "C"},
			edges: []DependencyEdge{
				{PredecessorTaskID: "A", SuccessorTaskID: "B"},
				{PredecessorTaskID: "B", SuccessorTaskID: "C"},
				{PredecessorTaskID: "C", SuccessorTaskID: "A"},
			},
		},
		{
			name:    "cycle in one component",
			taskIDs: []string{"A", "B", "C", "D"},
			edges: []DependencyEdge{
				{PredecessorTaskID: "A", SuccessorTaskID: "B"},
				{PredecessorTaskID: "B", SuccessorTaskID: "A"},
				{PredecessorTaskID: "C", SuccessorTaskID: "D"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewWorkflowGraph(tt.taskIDs, tt.edges)
			if !errors.Is(err, ErrDependencyCycle) {
				t.Fatalf("expected ErrDependencyCycle, got %v", err)
			}
		})
	}
}
