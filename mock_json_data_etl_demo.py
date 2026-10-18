import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="mock_json_etl_demo",
    schedule=None,  # Trigger manually for demo purposes
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "demo", "mock"],
)
def mock_json_etl_demo():

    @task()
    def extract() -> list[dict]:
        """Simulate extracting mock JSON raw data from an API or file."""
        raw_json_data = [
            {"id": "101", "name": "  alice ", "amount": "150.50", "status": "active"},
            {"id": "102", "name": "BOB", "amount": "200.00", "status": "inactive"},
            {"id": "103", "name": "charlie", "amount": "  50.75 ", "status": "active"},
            {"id": "104", "name": "Diana", "amount": "INVALID", "status": "active"},
        ]
        print(f"Extracted {len(raw_json_data)} raw records.")
        return raw_json_data

    @task()
    def transform(raw_data: list[dict]) -> list[dict]:
        """Clean, trim, parse, and filter the raw JSON records."""
        transformed_data = []

        for record in raw_data:
            # Skip inactive records
            if record.get("status") != "active":
                continue

            try:
                # Clean strings and parse numeric values safely
                cleaned_name = record["name"].strip().title()
                cleaned_amount = float(record["amount"].strip())

                transformed_data.append(
                    {
                        "record_id": record["id"],
                        "customer_name": cleaned_name,
                        "valid_amount": cleaned_amount,
                        "status": record["status"],
                    }
                )
            except (ValueError, TypeError):
                # Skip or log bad data rows (e.g., "INVALID" amount)
                print(
                    f"Skipping malformed record ID {record.get('id')}: {record.get('amount')}"
                )

        print(f"Transformed into {len(transformed_data)} clean records.")
        return transformed_data

    @task()
    def load(clean_data: list[dict]) -> None:
        """Simulate loading data into a target warehouse or database."""
        print("--- Loading data into target sink ---")
        for row in clean_data:
            print(f"INSERTED -> {row}")
        print(f"Successfully loaded {len(clean_data)} records.")

    # Define the DAG dependency chain
    raw_payload = extract()
    clean_payload = transform(raw_payload)
    load(clean_payload)


mock_json_etl_demo()

