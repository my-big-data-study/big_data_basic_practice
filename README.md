# data-storage-execrise

## Task
1. Write a python program to read the shakespeare text
2. Do word count with python pandas 
3. Save the word count result into local fs with parquet format and partitioned with capital letter. After running the program, there will be 27 partitions, A-Z and others. 

## Bonus:
### Given the single text file, how to speed up our program
1. Do a basic profile for your program and identify the slowest part
2. Try to improve the performance.
3. Add compression algorithm while write parquet file, and tuning & watch
### There are multiple text source files. 
1. Refactor your program to increase performance. (Hint: multithreading?) Feel free to make some assumptions.
### There are mulitple text source files with total size > 100 GB
1. What will happens when you running your program? Why? How to improve it (Hint: https://towardsdatascience.com/why-and-how-to-use-pandas-with-large-data-9594dda2ea4c)
