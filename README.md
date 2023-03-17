# mercury
Example of Spark in financial domain

## Problem Statement - 1 (Anonymize customer information)
Imagine you are working on a project where you have to process customer data and generate insights. Considering this data has customer information and to generate insights, multiple teams will be using this data. To ensure we handle customer information with care, and not make it visible to everyone on the team one requirement is to anonymize customer information before it's loaded into the warehouse for insights generation.

- You will get this data in CSV files which will have customer personal information like first_name, last_name, address, date_of_birth
- Write code to generate a CSV file containing first_name, last_name, address, date_of_birth
- Load generated CSV in the previous step, anonymize data, and output anonymized data to a different file
- Columns to anonymise are first_name, last_name and address


## Proposed solution
The app Mercury based on spark helps to generate anonymous data for each sensitive information using faker.
The application could be run from local or via container. The entry point is `go.sh`

## How to run?

```
./go.sh <command> [--] [options ...]

Commands:

linting   Static analysis, code style, etc.
precommit Run sensible checks before committing
run       Run the application
setup     Install dependencies
tests     Run tests
spark-docker-tests     Run spark docker tests
spark-docker-run     Run spark docker run
start-postgres     Start Postgres
stop-postgres     Stop Postgres
connect-to-local-postgres     Connect to local Postgres
Options are passed through to the sub-command.
```

---
**NOTE**

Postgres is an experimental feature. It is not in use for any application logic.

---