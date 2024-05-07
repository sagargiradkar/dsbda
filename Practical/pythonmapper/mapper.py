import sys
import io

input_stream = io.TextIOWrapper(sys.stdin.buffer)
for line in input_stream:
	words = line.split()
	for word in words:
		print("%s \t %s" %(word,1))
