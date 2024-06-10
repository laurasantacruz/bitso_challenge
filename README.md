# Bitso DE Challenge
This project runs on Airflow which can be run using docker. 

To run it, download the repo and type the command
`docker-compose up --build   `

You will need an env file for storing some Airflow variables. For Airflow variables you can use this command: `echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env`<br>
You will also need to create an AWS connection inside Airflow UI with your AWS keys. 

To shut down the application run: `docker-compose down`
