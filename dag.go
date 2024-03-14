package merkledag

import "hash"

func Add(store KVStore, node Node, h hash.Hash) []byte {

	// TODO 将分片写入到KVStore中，并返回Merkle Root
	var stack []Node
	stack = append(stack, node)
	for {
		node = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		// 检查节点是否为文件
		if fileNode, ok := node.(File); ok {
			// 如果是文件节点，将其字节放入 KVStore
			err := store.Put([]byte("fileData"), fileNode.Bytes())
			if err != nil {
				// 处理错误
				return nil
			}
			//计算当前 Merkle Root
			h.Sum(fileNode.Bytes())
		} else if dirNode, ok := node.(Dir); ok {
			// 如果是目录节点，则遍历文件并入栈
			dirIterator := dirNode.It()
			for dirIterator.Next() {
				fileOrDirNode := dirIterator.Node()
				stack = append(stack, fileOrDirNode)
			}
		}
	}
	// 计算并返回 Merkle Root
	return h.Sum(nil)
}

