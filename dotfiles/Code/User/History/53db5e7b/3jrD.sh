#/bin/bash

## This process orchestrate the PROD upgrade of BRIGHT
## It spins up the cloud infrastructure required 
## updates the data beneath QuicSight and runs the QA
## After all these finished, it shutdowns the resouces to avoid unwanted cost consequences.

# Export the credentials
# ? Credentials moved to the `./app/config/bright_us_prod_config.json` file

# The POSTGRES password needs to be exported according to AWS
# https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.AWSCLI.PostgreSQL.html
export PGPASSWORD='pvHZ&5y3A&nT8TQN'

export SHELL=/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Execute the ETL script
python3 -m ./etl/ps_etl.py --config_file_path ../config/bright_us_stage_config.json

# Execute the QA script
# cd ../qa
# python3 ./qa.py

