# docker-project
Exercise in Docker container + Spark + Python


## Description
this project preform analesys on average movies review from Stanford, Amazon product data.
http://jmcauley.ucsd.edu/data/amazon/links.html

Download the all the files to your local machine.
in the folder are 2 files types: python & note-book.
both with the same name: case_study_analyze	

## Installation
Must install docker container + docker compose
download the files to one folder.

## Usage
1) 	open note-book file with docker.
	- run the follwing command in cmd:
	- where  CWD = Your working directory
	docker run --rm -p 8888:8888 -v "CWD ":/home/jovyan/work jupyter/pyspark-notebook

2)	run the python file with docker.
	- run the follwing command in cmd:
	- where  CWD = Your working directory
		 date = date betweens years 97-2013, in format: yyyy-mm-dd
	docker run --rm  -v "CWD ":/job godatadriven/pyspark /job/case_study_analyze.py date
	

3)	execute the main.bat file in cmd with a date input.
	- edit the file extention from main.txt to => main.bat (cant sent .bat file on mail)
	- edit the run command in main.bat file to your CWD.
	- open cmd and navigate to the files directory.
	- run the follwing command in cmd:
	- where date = date betweens years 97-2013, in format: yyyy-mm-dd
	main.bat date
	

Exsamples:

1)
docker run --rm -p 8888:8888 -v "C:\Users\Yaron\Desktop\10_bis_assig":/home/jovyan/work jupyter/pyspark-notebook

2)
docker run --rm  -v "C:\Users\Yaron\Desktop\10_bis_assig":/job godatadriven/pyspark /job/case_study_analyze.py 2005-06-20

3)
main.bat 2005-06-20





