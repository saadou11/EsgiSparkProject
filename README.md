# EsgiSparkProject

### Download Datasets

before compiling make sure to download the 3 csv files at :

https://drive.google.com/drive/folders/17fd9maW5WoMQiU6m2YDmkCTEcneUFXG1?usp=sharing  

and copy them to :  
> src/main/resources/

### Compile 
 - to compile the project with Maven and create the Jar file use the command :
 
 `$ mvn install`

 
### Run 
to run the project use the command below : 

`$ {sparkPath}/bin/spark-submit --class Main target/EsgiSparkProject-1.0-SNAPSHOT.jar`
