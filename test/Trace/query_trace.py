#!/usr/bin/env python3
import os
import argparse
import sys
from datetime import datetime
try:
    from influxdb_client import InfluxDBClient
except ImportError:
    print("Error: 'influxdb-client' is not installed. Please install it using: pip install influxdb-client")
    sys.exit(1)

def setup_args():
    parser = argparse.ArgumentParser(description="Query traces from InfluxDB for CraneSched")
    
    # InfluxDB Connection Args
    parser.add_argument("--url", default=os.getenv("INFLUX_URL", "http://localhost:8086"), help="InfluxDB URL (env: INFLUX_URL)")
    parser.add_argument("--token", default=os.getenv("INFLUX_TOKEN", "your_token"), help="InfluxDB Token (env: INFLUX_TOKEN)")
    parser.add_argument("--org", default=os.getenv("INFLUX_ORG", "your_organization"), help="InfluxDB Organization (env: INFLUX_ORG)")
    parser.add_argument("--bucket", default=os.getenv("TRACE_BUCKET", "your_trace_bucket_name"), help="InfluxDB Bucket (env: TRACE_BUCKET)")
    
    # Query Filters
    parser.add_argument("--job-id", type=int, help="Filter by Job ID")
    parser.add_argument("--step-id", type=int, help="Filter by Step ID")
    parser.add_argument("--task-id", type=int, help="Filter by Task ID")
    parser.add_argument("--trace-id", type=str, help="Filter by Trace ID")
    parser.add_argument("--service", type=str, help="Filter by Service name (e.g. craned, cranectld)")
    
    # Time and Limits
    parser.add_argument("--minutes", type=int, default=60, help="Look back N minutes (default: 60)")
    parser.add_argument("--limit", type=int, default=50, help="Max number of spans to return (default: 50)")
    
    # Output format
    parser.add_argument("--verbose", "-v", action="store_true", help="Show all attributes")

    return parser.parse_args()

def build_flux_query(args):
    # Base query
    query = f'''from(bucket: "{args.bucket}")
    |> range(start: -{args.minutes}m)
    |> filter(fn: (r) => r["_measurement"] == "spans")'''

    # Apply filters
    # Note: Depending on how the data is stored (tag vs field), we might need to be careful.
    # Usually high cardinality data like IDs are fields, but low cardinality like service are tags.
    # However, the simple pivot below turns everything into columns so we can filter after pivot 
    # OR filter string tags before pivot for performance. 
    # Assuming attributes might be fields due to high dimensionality, we'll pivot first then filter 
    # to be safe, or check standard OpenTelemetry-to-Influx mappings.
    # For safety with generic schema, we pivot then filter.

    query += '''
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    # Use string conversion for ID filtering to ensure matching regardless of storage type (int vs string)
    if args.job_id is not None:
        query += f'|> filter(fn: (r) => exists r["job_id"] and string(v: r["job_id"]) == "{args.job_id}")\n'
    
    if args.step_id is not None:
        query += f'|> filter(fn: (r) => exists r["step_id"] and string(v: r["step_id"]) == "{args.step_id}")\n'
        
    if args.task_id is not None:
        query += f'|> filter(fn: (r) => exists r["task_id"] and string(v: r["task_id"]) == "{args.task_id}")\n'

    if args.trace_id is not None:
        query += f'|> filter(fn: (r) => r["trace_id"] == "{args.trace_id}")\n'

    if args.service is not None:
        query += f'|> filter(fn: (r) => r["service"] == "{args.service}")\n'

    query += f'''
    |> group(columns: [])
    |> sort(columns: ["_time"], desc: false)
    |> limit(n: {args.limit})
    '''
    
    return query

def main():
    args = setup_args()
    
    print(f"Connecting to {args.url}, Org: {args.org}, Bucket: {args.bucket}")
    
    try:
        client = InfluxDBClient(url=args.url, token=args.token, org=args.org)
        query_api = client.query_api()
        
        flux_query = build_flux_query(args)
        # print(f"Executing Query:\n{flux_query}") # Debug
        
        tables = query_api.query(flux_query)
        
        if not tables:
            print("No traces found matching the criteria.")
            return

        print(f"\nFound spans (Last {args.minutes} minutes):")
        print("-" * 100)
        # Header
        print(f"{'Time':<25} | {'Service':<10} | {'Operation':<20} | {'Duration (us)':<13} | {'Details'}")
        print("-" * 100)

        for table in tables:
            for record in table.records:
                vals = record.values
                
                time_str = record.get_time().strftime("%Y-%m-%d %H:%M:%S.%f")
                service = str(vals.get("service", "-"))
                name = str(vals.get("name", "-"))
                duration = vals.get("duration_us", 0)
                
                # Construct Details string from key IDs
                details_parts = []
                if "job_id" in vals and vals["job_id"] is not None:
                    details_parts.append(f"Job:{vals['job_id']}")
                if "step_id" in vals and vals["step_id"] is not None:
                    details_parts.append(f"Step:{vals['step_id']}")
                if "task_id" in vals and vals["task_id"] is not None:
                    details_parts.append(f"Task:{vals['task_id']}")
                
                details = ", ".join(details_parts)
                
                print(f"{time_str:<25} | {service:<10} | {name:<20} | {str(duration):<13} | {details}")
                
                if args.verbose:
                    # Print all other attributes indented
                    exclude = ["result", "table", "_start", "_stop", "_time", "_value", "_field", "_measurement", 
                               "trace_id", "span_id", "name", "duration_us", "service", "job_id", "step_id", "task_id"]
                    for k, v in vals.items():
                        if k not in exclude and v is not None:
                            print(f"    {k}: {v}")
                    print("") # Space after verbose block

    except Exception as e:
        print(f"\nError: {e}")
        print("Please check your InfluxDB connection settings and ensure the service is running.")

if __name__ == "__main__":
    main()
