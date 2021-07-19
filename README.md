# BDT-Project


## Project Objective 

This repository refers to the implementation and the structure of a big data system built to predict the usage and the status, for a given day and a specific time slot, of e-charging stations and plugs in the Italian region of Trentino Alto Adige.
The service, through a simple interface, is capable of providing to a user, who could be either a private driver or a maintenance technician, with the possibility to predict the status of a station up to the next 14 days.


## Project Structure 

- `retrievals` folder: it contains three files which refer respectively to three different data collection from three distinct API calls and include code for a preparatory data processing phase.

- `random_forest.py`: this file contains the code through which we instantiated our model, we trained and tested it, eventually saving the output in a .joblib file. 

- `redis_keys.py`: this code allows us to retrieve data from Big Query and populate our local Redis instance with key-value pairs regarding static data of our stations. 

- `docker-app` folder: in this folder you can find all the required files needed to build the Docker image, referring to the web application. Please note that the .joblib file contained in the `model` subfolder refers to a reduced version of the full model used in the dockerized service, this has been done because the full model's size is too large to be pushed on GitHub.

- `docker-full` folder: this folder includes the docker-compose.yml file which is necessary to run our full solution, composed by both the web application and the Redis instance.


## Instruction to run the service

1. pull the image on GitHub Packages 
2. download docker-full: [docker-full](https://downgit.github.io/#/home?url=https://github.com/GiulioMat/BDT-Project/tree/main/docker-full)
3. unzip it and run `docker-compose up` inside the folder
4. enter http://localhost:5000/ in a browser to see the application running

