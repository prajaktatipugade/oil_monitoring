import logging
import smtplib
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

# Static data mapping for tank capacity and minimum oil levels
STATION_CONFIG = {
    1: {"capacity": 1, "min_level": 2},
    2: {"capacity": 5, "min_level": 6},
    3: {"capacity": 9, "min_level": 10},
    4: {"capacity": 13, "min_level": 14},
    5: {"capacity": 17, "min_level": 18},
    6: {"capacity": 21, "min_level": 22},
}

# Modbus configuration for machines
MACHINE_CONFIG = [
    {"name": "KFPLU1/PM/VTL/06", "modbus_address": 0, "station_no": 1},
    {"name": "ALPL/PM/VMC/06", "modbus_address": 4, "station_no": 2},
    {"name": "SRF/PM/HMC/08", "modbus_address": 8, "station_no": 3},
    {"name": "SRF/PM/HMC/14", "modbus_address": 12, "station_no": 4},
    {"name": "SRF/PM/VMC/06", "modbus_address": 16, "station_no": 5},
    {"name": "SJI/PM/VTL/04", "modbus_address": 20, "station_no": 6},
]

# Modbus client setup
modbus_client = ModbusTcpClient('10.10.5.81', port=502, timeout=3)  # Set timeout here
modbus_client.connect()

# Function to send email alert
def send_email_alert(machine_name, oil_level, alert_message):
    from_email = "your_email@example.com"  # Your email
    to_email = "recipient_email@example.com"  # Recipient email
    password = "your_email_password"  # Your email password

    subject = f"Alert for {machine_name}: Oil Level Issue"
    body = f"Machine: {machine_name}\nOil Level: {oil_level}\n\nAlert: {alert_message}"

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.example.com', 587)  # Use appropriate SMTP server and port
        server.starttls()  # Secure the connection
        server.login(from_email, password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
        logging.info("Alert email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

# Function to generate alerts based on oil level
def generate_alerts(machine_name, oil_level, last_oil_level):
    if last_oil_level is not None:
        rate_of_change = abs(oil_level - last_oil_level)
        if oil_level <= 10:
            alert_message = f"CRITICAL ALERT! Oil level critically low at {oil_level}. Immediate refilling required."
            logging.critical(alert_message)
            send_email_alert(machine_name, oil_level, alert_message)
        elif oil_level <= 20:
            alert_message = f"Warning! Oil level is low at {oil_level}. Consider refilling soon."
            logging.warning(alert_message)
            send_email_alert(machine_name, oil_level, alert_message)
        elif rate_of_change >= 50:
            alert_message = f"Alert! Significant oil level change detected for {machine_name}. Changed by {rate_of_change}."
            logging.info(alert_message)
            send_email_alert(machine_name, oil_level, alert_message)
    else:
        logging.info(f"{machine_name}: Initial oil level reading: {oil_level}")

# Function to get the current shift (dummy example, implement your logic)
def get_current_shift():
    return 1  # This should be dynamic based on your shift logic

# Function to insert data into the station table
def insert_station_data(station_no, timestamp, actual_oil_level):
    """Insert oil level, tank capacity, and minimum level into the station table."""
    current_shift = get_current_shift()
    columns = {
        1: ("oil_level_shift1", "oil_level_shift2", "oil_level_shift3"),
        2: ("oil_level_shift2", "oil_level_shift1", "oil_level_shift3"),
        3: ("oil_level_shift3", "oil_level_shift1", "oil_level_shift2"),
    }
    col_current, col_other1, col_other2 = columns[current_shift]

    # Static tank capacity and minimum oil level
    capacity = STATION_CONFIG[station_no]["capacity"]
    min_level = STATION_CONFIG[station_no]["min_level"]

    # Prepare SQL query
    table_name = f"station_{station_no}"
    query = f"""
        INSERT INTO {table_name} 
        (timestamp, {col_current}, {col_other1}, {col_other2}, tank_capacity, min_oil_level)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = (timestamp, actual_oil_level, 0, 0, capacity, min_level)

    try:
        cursor.execute(query, values)
        conn.commit()
        logging.info(
            f"Inserted data into Station {station_no}: Timestamp: {timestamp}, "
            f"Oil Level: {actual_oil_level}, Tank Capacity: {capacity}, Min Level: {min_level}"
        )
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Failed to insert data into {table_name}: {e}")

# Function to read data from Modbus and process it
def read_modbus_and_process():
    """Read data from Modbus and insert into database."""
    for machine in MACHINE_CONFIG:
        try:
            # Read actual oil level
            address = machine["modbus_address"]
            station_no = machine["station_no"]
            result = modbus_client.read_holding_registers(address, 1)  # Removed `timeout`

            if result.isError():
                raise Exception(f"Modbus read error for {machine['name']} at address {address}")

            actual_oil_level = result.registers[0]
            timestamp = datetime.now()

            # Generate alert based on the oil level and previous level
            last_oil_level = None  # Implement logic to retrieve the last oil level from your database
            generate_alerts(machine['name'], actual_oil_level, last_oil_level)

            # Insert data into station table
            insert_station_data(station_no, timestamp, actual_oil_level)

        except Exception as e:
            logging.error(f"Error processing data for {machine['name']} at Station {station_no}: {e}")

# Main function to run the process
def main():
    read_modbus_and_process()

# Run the main function
if __name__ == "__main__":
    main()

# Close Modbus connection and database connection after use
modbus_client.close()
cursor.close()
conn.close()
