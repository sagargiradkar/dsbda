import sys
import io

current_word = None
current_count = 0

word = None

for line in sys.stdin:
	word,count = line.split('\t',1)
	count = int(count)
	
	if current_word == word:
		current_count += count
	else:
		if current_word:
			print('%s \t %s' %(current_word,current_count))
		current_word = word
		current_count = count
		
if current_word == word:
	print('%s \t %s' %(current_word,current_count))
