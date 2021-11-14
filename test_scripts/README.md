# Using the Config File

`config.txt` will contain the user defined MapReduce configurations necessary for running any of our test scripts. These configurations include the number of 
mappers, number reducers, the input file path, and a flag to indicate whether worker should be killed during test. 


The config file is a comma delimited file with the following format - 
```
<Number of mappers>, <Number of reducers>, <Input file path>, <Kill Index>
```
