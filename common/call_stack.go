package common

import "runtime"

func GetCallStackFunctionNames() []string {
	var functionNames []string

	pc := make([]uintptr, 20)

	n := runtime.Callers(2, pc)
	if n == 0 {
		return functionNames
	}

	pc = pc[:n]
	frames := runtime.CallersFrames(pc)

	for {
		frame, more := frames.Next()

		functionNames = append(functionNames, frame.Function)

		if !more {
			break
		}
	}

	return functionNames
}
