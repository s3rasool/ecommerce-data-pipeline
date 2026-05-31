import os
import json
from datetime import datetime, timezone

from airflow.models import Variable


def preprocess_behavior_json_files_incremental(
    input_dir,
    output_dir,
    processed_files_variable,
):
    os.makedirs(output_dir, exist_ok=True)

    required_fields = {
        "timestamp",
        "user_id",
        "event_type",
        "device",
        "session_id",
    }

    if not os.path.exists(input_dir):
        print(f"[ERROR] Input directory not found: {input_dir}")
        return

    # I assumed that the tracker task will keep list of files that proccessed(must change after the two other tasks completed and have changes)
    processed_files_raw = Variable.get(
        processed_files_variable,
        default_var="[]",
    )

    try:
        processed_files = set(json.loads(processed_files_raw))
    except json.JSONDecodeError:
        print(
            f"[WARN] Invalid Airflow Variable format for {processed_files_variable}. "
            "Resetting to empty list."
        )
        processed_files = set()

    input_files = sorted(
        [
            file_name
            for file_name in os.listdir(input_dir)
            if file_name.endswith(".json")
        ]
    )

    if not input_files:
        print(f"[INFO] No JSON/JSONL files found in: {input_dir}")
        return

    new_files = [
        file_name
        for file_name in input_files
        if file_name not in processed_files
    ]

    if not new_files:
        print("[INFO] No new behavior JSON files to process.")
        return

    print(f"[INFO] Found {len(new_files)} new file(s) to process.")

    total_files_processed = 0
    total_raw_events = 0
    total_valid_events = 0
    total_invalid_events = 0

    successfully_processed_files = []

    for file_name in new_files:
        input_path = os.path.join(input_dir, file_name)

        base_name, _ = os.path.splitext(file_name)
        output_file_name = f"{base_name}_clean.json"
        output_path = os.path.join(output_dir, output_file_name)

        file_raw_events = 0
        file_valid_events = 0
        file_invalid_events = 0

        print(f"[INFO] Processing file: {input_path}")

        try:
            with open(input_path, "r", encoding="utf-8") as input_file, \
                 open(output_path, "w", encoding="utf-8") as output_file:

                for line_number, line in enumerate(input_file, start=1):
                    line = line.strip()

                    if not line:
                        continue

                    file_raw_events += 1
                    total_raw_events += 1

                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as e:
                        file_invalid_events += 1
                        total_invalid_events += 1
                        print(
                            f"[WARN] Invalid JSON in {file_name} "
                            f"at line {line_number}: {e}"
                        )
                        continue

                    missing_fields = required_fields - set(event.keys())

                    if missing_fields:
                        file_invalid_events += 1
                        total_invalid_events += 1
                        print(
                            f"[WARN] Missing fields in {file_name} "
                            f"at line {line_number}: {missing_fields}"
                        )
                        continue

                    try:
                        event_timestamp = datetime.fromisoformat(event["timestamp"])
                    except ValueError:
                        file_invalid_events += 1
                        total_invalid_events += 1
                        print(
                            f"[WARN] Invalid timestamp in {file_name} "
                            f"at line {line_number}: {event.get('timestamp')}"
                        )
                        continue

                    event["timestamp"] = event_timestamp.isoformat()
                    event["event_date"] = event_timestamp.date().isoformat()
                    event["event_hour"] = event_timestamp.hour

                    event["user_id"] = str(event["user_id"]).strip()
                    event["event_type"] = str(event["event_type"]).strip().lower()
                    event["device"] = str(event["device"]).strip().lower()
                    event["session_id"] = str(event["session_id"]).strip()

                    if "duration_sec" in event and event["duration_sec"] is not None:
                        event["duration_sec"] = int(event["duration_sec"])

                    if "http_status" in event and event["http_status"] is not None:
                        event["http_status"] = int(event["http_status"])

                    if "results_count" in event and event["results_count"] is not None:
                        event["results_count"] = int(event["results_count"])

                    if "clicked_position" in event and event["clicked_position"] is not None:
                        event["clicked_position"] = int(event["clicked_position"])

                    if "quantity" in event and event["quantity"] is not None:
                        event["quantity"] = int(event["quantity"])

                    if "cart_total_items" in event and event["cart_total_items"] is not None:
                        event["cart_total_items"] = int(event["cart_total_items"])

                    if "cart_value" in event and event["cart_value"] is not None:
                        event["cart_value"] = float(event["cart_value"])

                    if "cart_items" in event and isinstance(event["cart_items"], list):
                        cleaned_cart_items = []

                        for item in event["cart_items"]:
                            if not isinstance(item, dict):
                                continue

                            cleaned_item = {
                                "product_id": str(item.get("product_id")).strip()
                                if item.get("product_id") is not None
                                else None,
                                "price": float(item.get("price", 0)),
                                "quantity": int(item.get("quantity", 0)),
                            }

                            cleaned_cart_items.append(cleaned_item)

                        event["cart_items"] = cleaned_cart_items

                    event["source_file"] = file_name
                    event["processed_at"] = datetime.now(timezone.utc).isoformat()

                    output_file.write(json.dumps(event, ensure_ascii=False) + "\n")

                    file_valid_events += 1
                    total_valid_events += 1

            successfully_processed_files.append(file_name)
            total_files_processed += 1

            print(f"[INFO] File processed successfully: {file_name}")
            print(f"[INFO] Raw events: {file_raw_events}")
            print(f"[INFO] Valid events: {file_valid_events}")
            print(f"[INFO] Invalid events: {file_invalid_events}")
            print(f"[INFO] Output written to: {output_path}")

        except Exception as e:
            print(f"[ERROR] Failed to process file {file_name}: {e}")

            if os.path.exists(output_path):
                os.remove(output_path)
                print(f"[INFO] Removed partial output file: {output_path}")

            continue

    if successfully_processed_files:
        processed_files.update(successfully_processed_files)

        Variable.set(
            processed_files_variable,
            json.dumps(sorted(processed_files)),
        )

        print(
            f"[INFO] Updated Airflow Variable: {processed_files_variable}"
        )

    print("**** TOTAL SUMMARY ****")
    print(f"[INFO] New files found: {len(new_files)}")
    print(f"[INFO] Files successfully processed: {total_files_processed}")
    print(f"[INFO] Total raw events: {total_raw_events}")
    print(f"[INFO] Total valid events: {total_valid_events}")
    print(f"[INFO] Total invalid events: {total_invalid_events}")
