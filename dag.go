package merkledag

import (
	"encoding/json"
	"fmt"
	"hash"
	"math"
)

const (
	KB          = 1 << 10
	ChunkSize   = 256 * KB
	MaxListLine = 4096
	BLOB        = "blob"
	LINK        = "link"
	TREE        = "tree"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	obj := &Object{}
	switch node.Type() {
	case FILE:
		obj = handleFile(node, store, h)
		break
	case DIR:
		obj = handleDir(node, store, h)
		break
	}
	JsonObj, _ := json.Marshal(obj)
	return computeHash(JsonObj, h)
}

func handleFile(node Node, store KVStore, h hash.Hash) *Object {
	obj := &Object{}
	FileNode, _ := node.(File)
	if FileNode.Size() > ChunkSize {
		numChunks := math.Ceil(float64(FileNode.Size()) / float64(ChunkSize))
		height := 0
		tmp := numChunks
		for {
			height++
			tmp /= MaxListLine
			if tmp == 0 {
				break
			}
		}
		obj, _ = dfsHandleFile(height, FileNode, store, 0, h)
	} else {
		obj.Data = FileNode.Bytes()
		putObjInStore(obj, store, h)
	}
	return obj
}

func handleDir(node Node, store KVStore, h hash.Hash) *Object {
	dirNode, _ := node.(Dir)
	iter := dirNode.It()
	treeObject := &Object{}
	for iter.Next() {
		node := iter.Node()
		switch node.Type() {
		case FILE:
			file := node.(File)
			tmp := handleFile(node, store, h)
			jsonMarshal, _ := json.Marshal(tmp)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: computeHash(jsonMarshal, h),
				Size: int(file.Size()),
				Name: file.Name(),
			})
			if tmp.Links == nil {
				treeObject.Data = append(treeObject.Data, []byte(BLOB)...)
			} else {
				treeObject.Data = append(treeObject.Data, []byte(LINK)...)
			}

			break
		case DIR:
			dir := node.(Dir)
			tmp := handleDir(node, store, h)
			jsonMarshal, _ := json.Marshal(tmp)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: computeHash(jsonMarshal, h),
				Size: int(dir.Size()),
				Name: dir.Name(),
			})
			treeObject.Data = append(treeObject.Data, []byte(TREE)...)
			break
		}
	}
	putObjInStore(treeObject, store, h)
	return treeObject
}

func computeHash(data []byte, h hash.Hash) []byte {
	h.Reset()
	h.Write(data)
	return h.Sum(nil)
}

func dfsHandleFile(height int, node File, store KVStore, start int, h hash.Hash) (*Object, int) {
	obj := &Object{}
	lenData := 0

	if height == 1 {
		// 处理剩余数据
		if len(node.Bytes())-start < ChunkSize {
			data := node.Bytes()[start:]
			obj.Data = append(obj.Data, data...)
			lenData = len(data)
			putObjInStore(obj, store, h)
			return obj, lenData
		}
	}

	// 处理多层分片
	for i := 1; i <= MaxListLine && start < len(node.Bytes()); i++ {
		var tmpObj *Object
		var tmpDataLen int

		if height > 1 {
			// 递归处理下一层数据
			tmpObj, tmpDataLen = dfsHandleFile(height-1, node, store, start, h)
		} else {
			// 处理当前层数据
			end := start + ChunkSize
			if end > len(node.Bytes()) {
				end = len(node.Bytes())
			}
			data := node.Bytes()[start:end]
			// 将数据存储到 KVStore
			blobObj := Object{
				Links: nil,
				Data:  data,
			}

			putObjInStore(&blobObj, store, h)
			// 更新 obj 中的 Links 和 Data
			jsonMarshal, _ := json.Marshal(blobObj)
			obj.Links = append(obj.Links, Link{
				Hash: computeHash(jsonMarshal, h),
				Size: len(data),
			})

			obj.Data = append(obj.Data, []byte(BLOB)...)
			tmpDataLen = len(data)
			start += ChunkSize
		}

		lenData += tmpDataLen
		jsonMarshal, _ := json.Marshal(tmpObj)
		obj.Links = append(obj.Links, Link{
			Hash: computeHash(jsonMarshal, h),
			Size: tmpDataLen,
		})
		obj.Data = append(obj.Data, []byte(LINK)...)

		if start >= len(node.Bytes()) {
			break
		}
	}

	// 将处理好的对象存储到 KVStore
	putObjInStore(obj, store, h)
	return obj, lenData
}

func putObjInStore(obj *Object, store KVStore, h hash.Hash) {
	value, err := json.Marshal(obj)
	if err != nil {
		fmt.Println("json.Marshal err:", err)
		return
	}
	hash := computeHash(value, h)
	store.Put(hash, value)
}

