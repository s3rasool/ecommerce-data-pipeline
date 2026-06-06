from datetime import datetime, timezone
import json
import os


def parse_timestamp(value):
    if value is None:
        raise ValueError("timestamp is None")

    value = str(value).strip()

    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")

    return datetime.fromisoformat(value)


def transform_json_files(
    extracted_dir,
    transformed_dir,
    cleanup_extracted=False,
    skip_existing=True,
):
    os.makedirs(transformed_dir, exist_ok=True)

    required_fields = {
        "timestamp",
        "user_id",
        "event_type",
        "device",
        "session_id",
    }

    if not os.path.exists(extracted_dir):
        raise ValueError(f"Extracted directory not found: {extracted_dir}")

    extracted_files = [
        file_name
        for file_name in os.listdir(extracted_dir)
        if file_name.endswith(".json")
    ]

    if not extracted_files:
        print(f"[INFO] No JSON files found in: {extracted_dir}")
        return

    total_files_seen = len(extracted_files)
    total_files_transformed = 0
    total_files_skipped = 0
    total_raw_events = 0
    total_valid_events = 0
    total_invalid_events = 0
    failed_files = []
    successfully_transformed_inputs = []

    print(f"[INFO] Found {total_files_seen} extracted file(s).")

    for file_name in extracted_files:
        input_path = os.path.join(extracted_dir, file_name)

        base_name, _ = os.path.splitext(file_name)
        output_file_name = f"{base_name}_transformed.jsonl"
        output_path = os.path.join(transformed_dir, output_file_name)
        temp_output_path = output_path + ".tmp"

        if skip_existing and os.path.exists(output_path):
            total_files_skipped += 1
            print(
                f"[INFO] Skipping already transformed file: {file_name} "
                f"because output exists: {output_path}"
            )
            continue

        file_raw_events = 0
        file_valid_events = 0
        file_invalid_events = 0

        print(f"[INFO] Transforming file: {input_path}")

        try:
            with open(input_path, "r", encoding="utf-8") as input_file:
                events = json.load(input_file)

            if not isinstance(events, list):
                raise ValueError(f"Extracted JSON file is not a list: {file_name}")

            with open(temp_output_path, "w", encoding="utf-8") as output_file:
                for index, event in enumerate(events, start=1):
                    file_raw_events += 1
                    total_raw_events += 1

                    if not isinstance(event, dict):
                        file_invalid_events += 1
                        total_invalid_events += 1
                        print(
                            f"[WARN] Invalid event type in {file_name} "
                            f"at record {index}. Expected dict."
                        )
                        continue

                    missing_fields = required_fields - set(event.keys())
                    if missing_fields:
                        file_invalid_events += 1
                        total_invalid_events += 1
                        print(
                            f"[WARN] Missing fields in {file_name} "
                            f"at record {index}: {missing_fields}"
                        )
                        continue

                    try:
                        event_timestamp = parse_timestamp(event["timestamp"])

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

                        event["extracted_file"] = file_name
                        event["transformed_at"] = datetime.now(timezone.utc).isoformat()

                        output_file.write(json.dumps(event, ensure_ascii=False) + "\n")

                        file_valid_events += 1
                        total_valid_events += 1

                    except Exception as e:
                        file_invalid_events += 1
                        total_invalid_events += 1
                        print(
                            f"[WARN] Failed to transform event in {file_name} "
                            f"at record {index}: {e}"
                        )
                        continue

            os.replace(temp_output_path, output_path)

            total_files_transformed += 1
            successfully_transformed_inputs.append(input_path)

            if file_valid_events == 0:
                print(f"[WARN] File produced 0 valid transformed events: {file_name}")

        except Exception as e:
            failed_files.append(file_name)
            print(f"[ERROR] Failed to transform file {file_name}: {e}")

            if os.path.exists(temp_output_path):
                os.remove(temp_output_path)
                print(f"[INFO] Removed partial temp transformed file: {temp_output_path}")

            if os.path.exists(output_path):
                os.remove(output_path)
                print(f"[INFO] Removed partial transformed file: {output_path}")

            continue

    print("**** TRANSFORMATION SUMMARY ****")
    print(f"[INFO] Extracted files found: {total_files_seen}")
    print(f"[INFO] Files transformed: {total_files_transformed}")
    print(f"[INFO] Files skipped: {total_files_skipped}")
    print(f"[INFO] Total raw events: {total_raw_events}")
    print(f"[INFO] Total valid transformed events: {total_valid_events}")
    print(f"[INFO] Total invalid skipped events: {total_invalid_events}")

    if failed_files:
        print("[WARN] Some files failed. Extracted folder will NOT be cleaned.")
        print(f"[WARN] Failed files: {failed_files}")
        raise ValueError(f"Transformation failed for files: {failed_files}")

    if cleanup_extracted:
        print(f"[INFO] Cleaning successfully transformed extracted files from: {extracted_dir}")

        for input_path in successfully_transformed_inputs:
            try:
                if os.path.exists(input_path):
                    os.remove(input_path)
                    print(f"[INFO] Removed: {input_path}")
            except Exception as e:
                print(f"[ERROR] Failed to remove {input_path}: {e}")
                raise

        print("[INFO] Extracted files cleaned successfully.")
