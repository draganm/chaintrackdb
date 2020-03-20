package store

import "fmt"

type BlockType byte

const (
	TypeUndefined BlockType = iota
	TypeCommit
	TypeDataLeaf
	TypeDataNode
	TypeBTreeNode
)

var BlockTypeNameMap = map[BlockType]string{
	TypeUndefined: "Undefined",
	TypeCommit:    "Commit",
	TypeDataLeaf:  "DataLeaf",
	TypeDataNode:  "DataNode",
	TypeBTreeNode: "BTreeNode",
}

func (s BlockType) String() string {
	tp, found := BlockTypeNameMap[s]
	if found {
		return tp
	}

	return fmt.Sprintf("Undefined type %d", s)
}
