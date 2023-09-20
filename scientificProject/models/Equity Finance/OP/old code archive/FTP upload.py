import csv
import io
import logging
import traceback
from datetime import datetime

import pandas as pd
import paramiko



logger = logging.getLogger()


class SFTPUploader:
    def __init__(self, hostname, username, password, port=22):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.ssh_client: paramiko.client.SSHClient = None
        self.sftp_client: paramiko.sftp_client.SFTPClient = None
        self.empty_file_suffix = 'DONE'

    def connect(self) -> None:
        """
        Connect to the remote server
        """
        self.connect_ssh_client()
        self.connect_sftp_client()

    def connect_ssh_client(self) -> None:
        try:
            # Create an SSH client
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_log_channel(logger.name)

            # Automatically add the host key
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to the remote server
            self.ssh_client.connect(hostname=self.hostname, username=self.username, password=self.password, port=self.port)

            print(f'SSH Connected to {self.hostname}')
        except Exception:
            logger.error(f'Failed to connect ssh to {self.hostname}, error: {traceback.format_exc()}')

    def connect_sftp_client(self) -> None:
        try:
            # Create an SFTP client from the SSH client
            self.sftp_client = self.ssh_client.open_sftp()

            print(f'SFTP Connected to {self.hostname}')
        except Exception:
            logger.error(f'Failed to connect sftp to {self.hostname}, error: {traceback.format_exc()}')

    def try_connect(self) -> None:
        """
        Connect to the remote server if not connected
        """

        if not self.check_ssh_connect_is_active():
            print(f'ssh_client is None, try to connect')
            self.connect_ssh_client()
            self.connect_sftp_client()

    def check_file_size(self, remote_file_path: str, expected_size: int) -> bool:
        self.try_connect()
        try:
            file_attributes = self.sftp_client.stat(remote_file_path)
            print(f'{remote_file_path}, file exists')
            print(f'file_attributes: {file_attributes}')

            # Get the file size from the attributes
            file_size = file_attributes.st_size

            # Compare the file size with the expected size
            return file_size == expected_size

        except IOError:
            print(f'{remote_file_path}, File does not exist')

        return False

    def upload_file(self, local_file_path: str, remote_file_path: str) -> None:
        """
        Upload a file to the remote server

        :param local_file_path: local file path
        :param remote_file_path: remote file path
        """
        self.try_connect()

        # Upload the file
        self.sftp_client.put(local_file_path, remote_file_path)

        print(f'Uploaded {local_file_path} to {remote_file_path}')

    def upload_list_as_csv(self, data_list: list[str], remote_file_path: str) -> int:
        """
        Upload a list of data as a CSV file to the remote server

        :param data_list: list of data
        :param remote_file_path: remote file path
        """
        self.try_connect()

        # Create a file-like object in memory
        csv_file = io.StringIO()

        # Create a CSV writer
        writer = csv.writer(csv_file)

        # Write the data rows
        for data in data_list:
            writer.writerow([data])

        # Set the file position to the beginning
        csv_file.seek(0)

        # Upload the file-like object to the SFTP server
        self.sftp_client.putfo(csv_file, remote_file_path)

        print(f'Uploading {len(data_list)} rows to {remote_file_path}')

        csv_file_size = csv_file.tell()

        return csv_file_size

    def upload_df_as_csv(self, data_df: pd.DataFrame, remote_file_path: str) -> int:
        """
        Upload a list of data as a CSV file to the remote server

        :param data_df: data in dataframe
        :param remote_file_path: remote file path
        """
        self.try_connect()

        # Create a file-like object in memory
        csv_file = io.StringIO()

        # Df to csv
        data_df.to_csv(csv_file, index=False)

        # Set the file position to the beginning
        csv_file.seek(0)

        # Upload the file-like object to the SFTP server
        self.sftp_client.putfo(csv_file, remote_file_path)

        print(f'Uploading {len(data_df)} rows to {remote_file_path}')

        csv_file_size = csv_file.tell()

        return csv_file_size

    def upload_empty_file_with_suffix_done(self, remote_file_path: str) -> str:
        """
        Upload an empty file with a suffix of .DONE to the remote server

        :param remote_file_path: remote file path
        :return: remote file path with suffix
        """
        self.try_connect()

        # Create an empty file-like object in memory
        empty_file = io.StringIO()

        # Create the remote file path with the suffix
        remote_file_path_with_suffix = remote_file_path + '.' + self.empty_file_suffix

        # Upload the file-like object to the SFTP server
        # Since sftp server script will move the file to another folder immediately, we need to set confirm=False
        self.sftp_client.putfo(empty_file, remote_file_path_with_suffix, confirm=False)

        print(f'Uploaded empty file with suffix .{self.empty_file_suffix} to {remote_file_path_with_suffix}')

        return remote_file_path_with_suffix

    def disconnect(self) -> None:
        """
        Disconnect from the remote server
        """
        if self.ssh_client is None or not self.check_ssh_connect_is_active():
            print(f'ssh_client is None or not active, already disconnected')
            return
        # Close the SFTP session and SSH connection
        self.sftp_client.close()
        self.ssh_client.close()

        print(f'Disconnected from {self.hostname}')

    def check_ssh_connect_is_active(self) -> bool:
        if self.ssh_client is not None and self.ssh_client.get_transport() is not None:
            return self.ssh_client.get_transport().is_active()
        return False




sftp_uploader = SFTPUploader('sftp.toratrading.com',
                             'polymer',
                             '2a1z7Gu$vKXU',
                             22)

try:
    sftp_uploader.connect()
    today = datetime.today()
    # today = today - timedelta(days=1)  #use when need try backdate
    # today = datetime.strptime('2023-03-27', '%Y-%m-%d') #only use to backdate specific date
    today_text = today.strftime('%Y%m%d')
    filename = today_text + '_op_swap_netting_tora_upload.csv'
    df = pd.read_csv('P:\All\FinanceTradingMO/' + filename)
    csv_file_size = sftp_uploader.upload_df_as_csv(df, '/incoming/' + filename)
    # csv_file_size = sftp_uploader.upload_df_as_csv(df,'/incoming/test.csv' )
    print(csv_file_size)
  # do stuff here
finally:
    sftp_uploader.disconnect()


