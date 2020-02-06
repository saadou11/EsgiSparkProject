# EsgiSparkProject

### Download Datasets

Before compiling make sure to download the 3 csv files at :

https://drive.google.com/drive/folders/17fd9maW5WoMQiU6m2YDmkCTEcneUFXG1?usp=sharing


### Edit Path
 - Copy your 3 csv files somewhere
 - Edit ***application.properties*** by adding ***YOUR OWN PATH*** to csv files

### Compile
 - To compile the project with Maven and create the Jar file use the command :
 
 `$ mvn install`

 
### Run
 - To run the project use the command below :

`$ ./spark-submit.sh PATH/TO/YOUR/CSV/FILES`
