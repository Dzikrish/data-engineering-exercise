import sys
import pandas

#mencetak argumen
print (sys.argv)

#argumen 0 adalah nama file
#argumen 1 berisi argumen pertama yang sebenarnya kita perlukan
day = sys.argv[1]

#tampilkan kalimat dengan argumen
print(f'job finished successfully for day = {day}')