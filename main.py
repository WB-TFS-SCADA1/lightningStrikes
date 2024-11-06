import pandas as pd
import numpy as np
from geopy.distance import geodesic
from typing import List, Tuple, Dict
from datetime import datetime, timedelta
import pytz
from dotenv import dotenv_values
import csv
import pathlib
import os
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import smtplib, ssl
from email import encoders
import logging
from logging.config import dictConfig
import traceback

if getattr(sys, 'frozen', False):
    currentDir = os.path.dirname(sys.executable)
    currentFile = os.path.basename(sys.executable)
elif __file__:
    currentDir = pathlib.Path(__file__).parent.resolve()
    currentFile = os.path.basename(__file__)
else:
    currentDir = pathlib.Path(__file__).parent.resolve()
    currentFile = os.path.basename(__file__)

config = dotenv_values(f"{currentDir}/.env")



def getLogConf(logName):
    dictConf = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
                'datefmt': '%Y-%m-%dT%H:%M:%S',
            },
        },
        'handlers': {
            'default': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'stream': sys.stderr,
            },
            'rotating_to_file': {
                'level': 'DEBUG',
                'class': "logging.handlers.RotatingFileHandler",
                'formatter': 'standard',
                "filename": logName,
                "maxBytes": 10000000,
                "backupCount": 2,
            },
        },
        'loggers': {
            '': {
                'handlers': ['default', 'rotating_to_file'],
                'level': 'DEBUG',
                'propagate': True
            }
        }
    }
    return dictConf


logName = f'{currentDir}/logs/{currentFile.replace(".py", "")}.log'
loggingConfig = getLogConf(logName)
logging.config.dictConfig(loggingConfig)
logger = logging.getLogger(__name__)

logger.info(currentDir)


def emailReport(filename, emailMsg, emailSubject):
    logger.info(f"Attempting to send email report: {emailSubject}")

    try:
        # Create message
        msg = MIMEMultipart()
        msg['Subject'] = emailSubject
        msg['From'] = config['emailUser']
        msg.attach(MIMEText(emailMsg, 'html'))

        # Attach file
        logger.info(f"Attaching file: {filename}")
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(filename)}",
        )
        msg.attach(part)

        context = ssl.create_default_context()

        logger.info(f"Connecting to SMTP server: {config['smtpServer']}:{config['smtpPort']}")

        # Connect to server with timeout
        with smtplib.SMTP(config['smtpServer'], int(config['smtpPort']), timeout=120) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()

            logger.info("Logging into SMTP server")
            server.login(config['emailUser'], config['emailPass'])

            # Send the email
            logger.info("Sending email")
            server.sendmail(
                config['emailUser'],
                ["chris.morris@h2obridge.com", "joey.philippi@h2obridge.com", "charles.lame@h2obridge.com"],
                msg.as_string()
            )
            logger.info("Email sent successfully")

    except ssl.SSLError as e:
        logger.error(f"SSL Error occurred: {str(e)}")
        raise
    except smtplib.SMTPException as e:
        logger.error(f"SMTP Error occurred: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error sending email: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def validate_coordinates(lat: float, lon: float) -> bool:
    """
    Validate that coordinates are within valid ranges
    """
    try:
        lat_float = float(lat)
        lon_float = float(lon)
        return -90 <= lat_float <= 90 and -180 <= lon_float <= 180
    except (ValueError, TypeError):
        return False

def load_data(site_query: str, strikes_query: str, query_params: Tuple) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load data from SQL Server database
    """
    import pyodbc
    
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={config["sqlServer"]};'
        f'DATABASE={config["sqlDatabase"]};'
        f'UID={config["sqlUser"]};'
        f'PWD={config["sqlPassword"]}'
    )
    
    # Load the data
    sites_df = pd.read_sql(site_query, conn)
    strikes_df = pd.read_sql(strikes_query, conn, params=query_params)
    
    # Convert UTC timestamps to Central Time
    central = pytz.timezone('America/Chicago')
    strikes_df['Timestamp'] = pd.to_datetime(strikes_df['Timestamp'])
    strikes_df['Timestamp'] = strikes_df['Timestamp'].apply(
        lambda x: x.replace(tzinfo=pytz.UTC).astimezone(central)
    )
    
    conn.close()
    return sites_df, strikes_df

def get_strikes_for_site(site_row: pd.Series, strikes_df: pd.DataFrame, radius_miles: float) -> List[Dict]:
    """
    Get all strikes within radius of a site with their details
    """
    try:
        site_lat = float(site_row['Latitude'])
        site_lon = float(site_row['Longitude'])
        
        if not validate_coordinates(site_lat, site_lon):
            print(f"Warning: Invalid coordinates for site {site_row['SiteName']}: {site_lat}, {site_lon}")
            return []
        
        site_coords = (site_lat, site_lon)  # Note: order is (lat, lon)
    except (ValueError, TypeError) as e:
        print(f"Error parsing coordinates for site {site_row['SiteName']}: {e}")
        return []
    
    strikes_in_radius = []
    
    for _, strike in strikes_df.iterrows():
        try:
            strike_lat = float(strike['Latitude'])
            strike_lon = float(strike['Longitude'])
            
            if not validate_coordinates(strike_lat, strike_lon):
                continue
                
            strike_coords = (strike_lat, strike_lon)  # Note: order is (lat, lon)
            distance = geodesic(site_coords, strike_coords).miles
            
            if distance <= radius_miles:
                strikes_in_radius.append({
                    'latitude': strike_lat,
                    'longitude': strike_lon,
                    'timestamp': strike['Timestamp'],
                    'distance': distance
                })
        except (ValueError, TypeError) as e:
            print(f"Error processing strike coordinates: {e}")
            continue
    
    # Sort strikes by timestamp
    return sorted(strikes_in_radius, key=lambda x: x['timestamp'])

def create_detailed_report(sites_df: pd.DataFrame, strikes_df: pd.DataFrame, radii: List[float], filename: str) -> int:
    """
    Create a detailed report with strikes listed under each site
    Returns the number of sites with strikes
    """
    sites_with_strikes = 0
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Site Name', 'Latitude', 'Longitude', 
                        f'Strikes ({radii[0]} mi)'])
        
        for _, site in sites_df.iterrows():
            # Get strikes for each radius
            strikes_1mi = get_strikes_for_site(site, strikes_df, radii[0])
            
            # Skip sites with no strikes in either radius
            if not strikes_1mi:
                continue
                
            sites_with_strikes += 1
            
            # Write site summary row
            writer.writerow([
                site['SiteName'],
                site['Latitude'],
                site['Longitude'],
                len(strikes_1mi)
            ])
            
            # Write 1-mile radius strikes
            if strikes_1mi:
                writer.writerow(['Strikes within 1 mile:'])
                for strike in strikes_1mi:
                    writer.writerow([
                        '  Strike',
                        strike['latitude'],
                        strike['longitude'],
                        strike['timestamp'].strftime('%Y-%m-%d %I:%M:%S %p %Z'),
                        f"{strike['distance']:.2f} miles"
                    ])
            
            # Add blank line between sites
            writer.writerow([])
    
    return sites_with_strikes

def main():
    try:
        # SQL queries
        site_query = """
        SELECT SiteName, Latitude, Longitude 
        FROM site 
        WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL and site.type not in ('Remote') and site.enabled = 1
        """

        strikes_query = """
        SELECT Latitude, Longitude, [Timestamp]
        FROM LightningStrikes 
        WHERE Latitude IS NOT NULL 
        AND Longitude IS NOT NULL 
        AND [Timestamp] >= DATEADD(day, -7, GETDATE())
        """

        # Load data
        sites_df, strikes_df = load_data(site_query, strikes_query, ())

        # Create report for 1 and 5 mile radii
        radii = [1.0]

        # Generate filename with timestamp
        timestamp = datetime.now(pytz.timezone('America/Chicago')).strftime('%Y%m%d_%H%M%S')
        filename = (f'{currentDir}/detailed_lightning_report_{timestamp}.csv')

        # Create the detailed report and get count of sites with strikes
        sites_with_strikes = create_detailed_report(sites_df, strikes_df, radii, filename)

        # Print summary
        central_now = datetime.now(pytz.timezone('America/Chicago'))
        utc_now = central_now.astimezone(pytz.UTC)

        logger.info(f"\nReport saved as: {filename}")
        logger.info(f"Total lightning strikes analyzed: {len(strikes_df)}")
        logger.info(f"Total sites analyzed: {len(sites_df)}")
        logger.info(f"Sites with strikes: {sites_with_strikes}")
        logger.info(f"Sites without strikes: {len(sites_df) - sites_with_strikes}")
        logger.info(f"Date range: Last 7 days from {central_now.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
        logger.info(f"Query start time (UTC): {utc_now - timedelta(days=7)}")
        emailReport(filename,
                        f"Lightning report attached.",
                        f"Weekly Lightning Strike Report")
    except:
        print(f"Script {currentFile} has failed:\n {traceback.format_exc()}", 'html')

        logger.info(traceback.format_exc())
        msg = MIMEMultipart()
        msg['Subject'] = f"Script {currentFile} - Failed"
        msg['From'] = config['emailUser']
        context = ssl.create_default_context()
        server = smtplib.SMTP(config['smtpServer'], int(config['smtpPort']), timeout=120)
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(config['emailUser'], config['emailPass'])
        try:
            logger.info(f"Program failed")
            logger.info(f"error: {traceback.format_exc()}")

            msgErr = MIMEMultipart()
            msgErr['Subject'] = f"Script {currentFile} - Failed"
            msgErr['To'] = config['errorEmails']
            msgErr['From'] = config['emailUser']
            msgErr.attach(MIMEText(f"Script {currentFile} has failed:\n {traceback.format_exc()}", 'html'))
            server.sendmail(config['emailUser'], config['errorEmails'], msgErr.as_string())
            server.quit()
        except:
            msgErr = MIMEMultipart()
            msgErr['Subject'] = f"Script {currentFile} - Failed"
            msgErr['To'] = config['errorEmails']
            msgErr['From'] = config['emailUser']
            msgErr.attach(MIMEText(f"Script {currentFile} has failed:\n {traceback.format_exc()}", 'html'))
            server.sendmail(config['emailUser'], config['errorEmails'], msgErr.as_string())
            server.quit()

if __name__ == "__main__":
    main()