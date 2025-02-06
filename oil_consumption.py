import logging
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
from datetime import datetime
from pymodbus.client import ModbusTcpClient

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database connection
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="oil_monitoring",
    user="postgres",
    password="1234"
)
cursor = conn.cursor()

# Modbus client setup
MODBUS_IP = "10.10.5.81"
MODBUS_PORT = 502
modbus_client = ModbusTcpClient(MODBUS_IP, port=MODBUS_PORT, timeout=3)
modbus_client.connect()

# Modbus address mapping per machine
MACHINE_CONFIG = [
    {"name": "KFPLU1/PM/VTL/06", "modbus_oil_level": 0, "modbus_capacity": 1, "modbus_min_level": 2, "station_no": 1},
    {"name": "ALPL/PM/VMC/06", "modbus_oil_level": 4, "modbus_capacity": 5, "modbus_min_level": 6, "station_no": 2},
    {"name": "SRF/PM/HMC/08", "modbus_oil_level": 8, "modbus_capacity": 9, "modbus_min_level": 10, "station_no": 3},
    {"name": "SRF/PM/HMC/14", "modbus_oil_level": 12, "modbus_capacity": 13, "modbus_min_level": 14, "station_no": 4},
    {"name": "SRF/PM/VMC/06", "modbus_oil_level": 16, "modbus_capacity": 17, "modbus_min_level": 18, "station_no": 5},
    {"name": "SJI/PM/VTL/04", "modbus_oil_level": 20, "modbus_capacity": 21, "modbus_min_level": 22, "station_no": 6},
]

# Function to generate alerts
def generate_alerts(machine_name, oil_level, last_oil_level):
    if last_oil_level is not None:
        rate_of_change = abs(oil_level - last_oil_level)
        if oil_level <= 10:
            alert_message = f"CRITICAL ALERT! Oil level critically low at {oil_level}. Immediate refilling required."
            logging.critical(alert_message)
        elif oil_level <= 20:
            alert_message = f"Warning! Oil level is low at {oil_level}. Consider refilling soon."
            logging.warning(alert_message)
        elif rate_of_change >= 50:
            alert_message = f"Alert! Significant oil level change detected for {machine_name}. Changed by {rate_of_change}."
            logging.info(alert_message)
        else:
            return  # No significant alert, return early
    else:
        logging.info(f"{machine_name}: Initial oil level reading: {oil_level}")

# Function to get current shift
def get_current_shift():
    return 1  # Implement shift logic

# Function to insert data into the station table
def insert_station_data(station_no, timestamp, actual_oil_level, tank_capacity, min_oil_level):
    current_shift = get_current_shift()
    columns = {
        1: ("oil_level_shift1", "oil_level_shift2", "oil_level_shift3"),
        2: ("oil_level_shift2", "oil_level_shift1", "oil_level_shift3"),
        3: ("oil_level_shift3", "oil_level_shift1", "oil_level_shift2"),
    }
    col_current, col_other1, col_other2 = columns[current_shift]

    table_name = f"station_{station_no}"
    query = f"""
        INSERT INTO {table_name} 
        (timestamp, {col_current}, {col_other1}, {col_other2}, tank_capacity, min_oil_level)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = (timestamp, actual_oil_level, 0, 0, tank_capacity, min_oil_level)

    try:
        cursor.execute(query, values)
        conn.commit()
        logging.info(
            f"Inserted data into Station {station_no}: Timestamp: {timestamp}, "
            f"Oil Level: {actual_oil_level}, Tank Capacity: {tank_capacity}, Min Level: {min_oil_level}"
        )
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Failed to insert data into {table_name}: {e}")

# Function to read data from Modbus and process it
def read_modbus_and_process():
    for machine in MACHINE_CONFIG:
        try:
            address_oil_level = machine["modbus_oil_level"]
            address_capacity = machine["modbus_capacity"]
            address_min_level = machine["modbus_min_level"]
            station_no = machine["station_no"]

            result_oil = modbus_client.read_holding_registers(address_oil_level, 1)
            result_capacity = modbus_client.read_holding_registers(address_capacity, 1)
            result_min = modbus_client.read_holding_registers(address_min_level, 1)

            if result_oil.isError() or result_capacity.isError() or result_min.isError():
                raise Exception(f"Modbus read error for {machine['name']} at address {address_oil_level}")

            actual_oil_level = result_oil.registers[0] / 10
            tank_capacity = result_capacity.registers[0] / 10
            min_oil_level = result_min.registers[0] / 10

            timestamp = datetime.now()

            # Retrieve last oil level from the database (implementation needed)
            last_oil_level = None  # Implement fetching last oil level logic

            generate_alerts(machine['name'], actual_oil_level, last_oil_level)
            insert_station_data(station_no, timestamp, actual_oil_level, tank_capacity, min_oil_level)

        except Exception as e:
            logging.error(f"Error processing data for {machine['name']} at Station {station_no}: {e}")

# Main function with real-time looping
def main():
    try:
        while True:
            logging.info("Reading Modbus data...")
            read_modbus_and_process()
            time.sleep(300)  # Wait 5 seconds before the next reading
    except KeyboardInterrupt:
        logging.info("Stopping real-time monitoring...")
    finally:
        # Close Modbus and database connections gracefully
        modbus_client.close()
        cursor.close()
        conn.close()

# Run the main function
if __name__ == "__main__":
    main()
