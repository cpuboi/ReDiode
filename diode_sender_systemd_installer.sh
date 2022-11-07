#!/bin/bash
set -e
INSTALLDIR="/opt/diode_sender/"
echo "Creating directory"
sudo mkdir  -p $INSTALLDIR


# Move files
sudo cp ./diode_sender.py ./diode_sender.conf ./diode_utils.py $INSTALLDIR


# -u implies unbuffered output

echo "[Unit]
Description=Redis diode sender
After=multi-user.target

[Service]
Type=idle
ExecStart=/usr/bin/python3 -u /opt/diode_sender/diode_sender.py
Restart=on-failure
RestartSec=20s

StandardOutput=append:/var/log/diode_sender.log
StandardError=inherit

[Install]
WantedBy=multi-user.target" | sudo tee /lib/systemd/system/diode-sender.service

sudo chmod 644 /lib/systemd/system/diode-sender.service

sudo systemctl daemon-reload
sudo systemctl enable diode-sender.service
sudo systemctl start diode-sender
