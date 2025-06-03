from db_setup import setup_database
from mqtt_listener import main as start_mqtt

if __name__ == "__main__":
    setup_database()
    start_mqtt()
