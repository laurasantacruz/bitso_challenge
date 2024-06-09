# Bitso DE Challenge
This project runs on Airflow which can be run using docker. 

To run it, download the repo and type the command
`docker-compose up --build   `

You will need an env file for storing Keys and some Airflow variables. For Airflow variables you can use this command: `echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env`
The variables that you'll also need are from AWS: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and from Bitso API: API_KEY, API_SECRET (https://docs.bitso.com/bitso-api/docs/2-generate-your-api-credentials).
