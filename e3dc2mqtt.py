from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from datetime import datetime
import configargparse
import paho.mqtt.client as mqtt
import time
import json
import random


parser = configargparse.ArgParser(description='Bridge between E3/DC and MQTT')
parser.add_argument('--mqtt-host', env_var='MQTT_HOST',required=True, help='MQTT server address')
parser.add_argument('--mqtt-port', env_var='MQTT_PORT', default=1883, type=int, help='Defaults to 1883')
parser.add_argument('--mqtt-topic', env_var='MQTT_TOPIC', default='e3dc/', help='Topic prefix to be used for subscribing/publishing. Defaults to "e3dc/"')
parser.add_argument('--mqtt-user', env_var='MQTT_USER', default='', help='Username for authentication (optional)')
parser.add_argument('--mqtt-pass', env_var='MQTT_PASS', default='', help='Password for authentication (optional)')
parser.add_argument('--e3dc-host', env_var='E3DC_HOST', required=True, help='IP address of the E3/DC System')
parser.add_argument('--e3dc-port', env_var='E3DC_PORT', default=502, help='Modbus port of the E3/DC System. Defaults to 502')
parser.add_argument('--poll-intervall', env_var='POLL_INTERVALL', default=5, type=int, help='Poll intervall in seconds. Defaults to 5')

args=parser.parse_args()

def connect_client(host, port):
    client = ModbusClient(host, port=port)
    connection = client.connect()
    if connection:
        return client
    else:
        return False

def read_registers(client):
    start_register = 40000
    register_count = 127
    UNIT = 0x1
    response = client.read_holding_registers(start_register, register_count, unit=UNIT)
    if not response.isError():
        decoder = BinaryPayloadDecoder.fromRegisters(
            response.registers,
            byteorder=Endian.Big, wordorder=Endian.Little
        )
        return decoder
    else:
        return False

def decode_data(decoder):
    data = {
        'info': {},
        'power': {},
        'wallbox': {},
        'solar': {},
        'meter': {},
        'error': ''
    }
    # 40001 Magicbyte - ModBus ID (Immer 0xE3DC)
    modbus_id = hex(decoder.decode_16bit_uint())
    if modbus_id == hex(0xe3dc):
        # INFO
        # 40002 ModBus-Firmware-Version
        firmware_minor = decoder.decode_8bit_uint()
        firmware_major = decoder.decode_8bit_uint()
        data['info']['modbus_firmware_version'] = str(firmware_major) + '.' + str(firmware_minor)
        # 40003 Anzahl unterstützter Register
        data['info']['modbus_register_count'] = decoder.decode_16bit_uint()
        # 40004 Hersteller: „E3/DC GmbH“
        data['info']['manufacturer'] = decoder.decode_string(32).decode('utf-8').rstrip('\x00')
        # 40020 Modell, z. B.: „S10 E AIO“ oder „Q10“
        data['info']['model'] = decoder.decode_string(32).decode('utf-8').rstrip('\x00')
        # 40036 Seriennummer, z. B.: „S10-12345678912“
        data['info']['serial_number'] = decoder.decode_string(32).decode('utf-8').rstrip('\x00')
        # 40052 Firmware Release, z. B.: “S10_2020_04”, „Q10_2020_04“ oder “P10_2020_04”
        data['info']['firmware_version'] = decoder.decode_string(32).decode('utf-8').rstrip('\x00')
        # POWER
        # 40068 Photovoltaik-Leistung in Watt
        data['power']['solar_power'] = decoder.decode_32bit_int()
        # 40070 Batterie-Leistung in Watt (negative Werte = Entladung)
        data['power']['battery_power'] = decoder.decode_32bit_int()
        # 40072 Hausverbrauchs-Leistung in Watt
        data['power']['home_power'] = decoder.decode_32bit_int()
        # 40074 Leistung am Netzübergabepunkt in Watt (negative Werte = Einspeisung)
        data['power']['grid_power'] = decoder.decode_32bit_int()
        # 40076 Leistung aller zusätzlichen Einspeiser in Watt
        data['power']['generator_power'] = decoder.decode_32bit_int()
        # 40078 Leistung der Wallbox in Watt
        data['power']['wallbox_power'] = decoder.decode_32bit_int()
        # 40080 Solarleistung, die von der Wallbox genutzt wird in Watt
        data['power']['wallbox_solar_power'] = decoder.decode_32bit_int()
        # 40082 Autarkie und Eigenverbrauch in Prozent
        data['power']['autarky'] = decoder.decode_8bit_uint()
        data['power']['self_consumption'] = decoder.decode_8bit_uint()
        # 40083 Batterie-SOC in Prozent
        data['power']['battery_soc'] = decoder.decode_16bit_uint()
        # 40084 Emergency-Power Status
        # 0 = Notstrom wird nicht von Ihrem Gerät unterstützt (bei Geräten der älteren Gerätegeneration, z. B. S10-SP40, S10-P5002).
        # 1 = Notstrom aktiv (Ausfall des Stromnetzes)
        # 2 = Notstrom nicht aktiv
        # 3 = Notstrom nicht verfügbar
        # 4 = Motorschalter (Nur S10 E und S10 E PRO): Der Motorschalter befindet sich nicht in der richtigen Position, sondern wurde manuell ausgeschaltet oder nicht eingeschaltet.
        data['power']['emergency_power_status_raw'] = decoder.decode_16bit_uint()
        emergency_power_status_translated = [
            'Emergency power not supported',
            'Emergency power active',
            'Emergency power available',
            'Emergency power not available',
            'Emergency power switch off',
        ]
        data['power']['emergency_power_status'] = emergency_power_status_translated[data['power']['emergency_power_status_raw']]
        # 40085 EMS-Status
        ems_2 = decoder.decode_bits()
        ems_1 = decoder.decode_bits()
        data['power']['ems_battery_charge_locked'] = ems_1[0]
        data['power']['ems_battery_discharge_locked'] = ems_1[1]
        data['power']['ems_emergency_power_available'] = ems_1[2]
        data['power']['ems_weather_charge_delay_active'] = ems_1[3]
        data['power']['ems_grid_curtailment'] = ems_1[4]
        data['power']['ems_charge_blocking_time_active'] = ems_1[5]
        data['power']['ems_discharge_blocking_time_active'] = ems_1[6]
        # 40086 Reserved (E3/DC use only)
        reserved_1 = decoder.decode_16bit_int()
        # 40087 Reserved (E3/DC use only)
        reserved_2 = decoder.decode_16bit_uint()
        # WALLBOX
        # 40088 - 40095 WallBox_X_CTRL
        for wallbox_number in range(0, 8):
            number = str(wallbox_number)
            wallbox_bits = decoder.decode_bits()
            wallbox_bits[:0] = decoder.decode_bits()
            if wallbox_bits[0]:
                data['wallbox']['wallbox_' + number] = {}
                data['wallbox']['wallbox_' + number]['mode'] = 'solar' if wallbox_bits[1] else 'mix'
                data['wallbox']['wallbox_' + number]['charging'] = 'cancelled' if wallbox_bits[2] else 'available'
                data['wallbox']['wallbox_' + number]['status'] = 'charging' if wallbox_bits[3] else 'not charging'
                data['wallbox']['wallbox_' + number]['type_2_locked'] = wallbox_bits[4]
                data['wallbox']['wallbox_' + number]['type_2_plugged'] = wallbox_bits[5]
                data['wallbox']['wallbox_' + number]['type_f_active'] = wallbox_bits[6]
                data['wallbox']['wallbox_' + number]['type_f_plugged'] = wallbox_bits[7]
                data['wallbox']['wallbox_' + number]['type_f_locked'] = wallbox_bits[8]
                data['wallbox']['wallbox_' + number]['type_f_relay_active'] = wallbox_bits[9]
                data['wallbox']['wallbox_' + number]['type_2_relay_16a_3p_active'] = wallbox_bits[10]
                data['wallbox']['wallbox_' + number]['type_2_relay_32a_3p_active'] = wallbox_bits[11]
                data['wallbox']['wallbox_' + number]['phases'] = '1P' if wallbox_bits[12] else '3P'
                data['wallbox']['wallbox_' + number]['raw'] = wallbox_bits[0:13]
        # SOLAR
        # 40096 DC-Spannung an String 1 in Volt
        data['solar']['string_1_voltage'] = decoder.decode_16bit_uint()
        # 40097 DC-Spannung an String 2 in Volt
        data['solar']['string_2_voltage'] = decoder.decode_16bit_uint()
        # 40098 DC-Spannung an String 3 in Volt (wird nicht verwendet)
        string_3_voltage = decoder.decode_16bit_uint()
        # 40099 DC-Strom an String 1 in Ampere (Faktor 0.01)
        data['solar']['string_1_current'] = decoder.decode_16bit_uint() / 100
        # 40100 DC-Strom an String 2 in Ampere (Faktor 0.01)
        data['solar']['string_2_current'] = decoder.decode_16bit_uint() / 100
        # 40101 DC-Strom an String 3 in Ampere (Faktor 0.01) (wird nicht verwendet)
        string_3_current = decoder.decode_16bit_uint()
        # 40102 DC-Leistung an String 1 in Watt
        data['solar']['string_1_power'] = decoder.decode_16bit_uint()
        # 0103 DC-Leistung an String 2 in Watt
        data['solar']['string_2_power'] = decoder.decode_16bit_uint()
        # 40104 DC-Leistung an String 3 in Watt (wird nicht verwendet)
        string_3_power = decoder.decode_16bit_uint()
        # METER
        # 40105 - 40132 Leistungsmesser 0 - 6
        for meter_number in range(0, 5):
            number = str(meter_number)
            meter_type = decoder.decode_16bit_uint()
            p1_power = decoder.decode_16bit_int()
            p2_power = decoder.decode_16bit_int()
            p3_power = decoder.decode_16bit_int()
            meter_types = [
                '',
                'Main',
                'External production',
                'Bidirectional',
                'External consumption',
                'Farm',
                '',
                'Wallbox',
                'External farm',
                'Data logger',
                'Bypass',
            ]
            if meter_type > 0:
                data['meter']['meter_' + number] = {}
                data['meter']['meter_' + number]['type'] = meter_types[meter_type]
                data['meter']['meter_' + number]['type_raw'] = meter_type
                data['meter']['meter_' + number]['p1_power'] = p1_power
                data['meter']['meter_' + number]['p2_power'] = p2_power
                data['meter']['meter_' + number]['p3_power'] = p3_power
    else:
        data['error'] = 'no magic byte'
    return data

def main_loop(client):
    while True:
        error = ''

        if client != False and client.is_socket_open():
            decoder = read_registers(client)
            if decoder != False:
                data = decode_data(decoder)
                error = data['error']
            else:
                error = 'no modbus response'
        else:
            error = 'no modbus connection'

        if error != '':
            return error

        del data['error']
        mqtt_client.publish(args.mqtt_topic + 'data', json.dumps(data))

        time.sleep(args.poll_intervall)


client_id = f'e3dc-{random.randint(0, 1000)}'
mqtt_client = mqtt.Client(client_id)
if args.mqtt_user != '' or args.mqtt_pass != '':
    mqtt_client.username_pw_set(args.mqtt_user, args.mqtt_pass)
mqtt_client.connect(args.mqtt_host, args.mqtt_port)
# mqtt_client.disconnect()
mqtt_client.loop_start()


while True:
    client = connect_client(args.e3dc_host, args.e3dc_port)
    error = main_loop(client)
    if client != False:
        client.close()
    print('ERROR: ' + error)
    mqtt_client.publish(args.mqtt_topic + 'error_time', datetime.today().strftime('%Y-%m-%d-%H:%M:%S'))
    mqtt_client.publish(args.mqtt_topic + 'error', error)
    time.sleep(args.poll_intervall)
