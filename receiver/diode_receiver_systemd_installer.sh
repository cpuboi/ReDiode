#!/bin/bash
set -e
INSTALLDIR="/opt/diode_receiver/"
echo "Creating directory"
sudo mkdir  -p $INSTALLDIR


# Move files
sudo cp ./diode_receiver.py ./diode_receiver.conf ./diode_utils.py  $INSTALLDIR


# -u implies unbuffered output

echo "[Unit]
Description=Redis diode sender
After=multi-user.target

[Service]
Type=idle
ExecStart=/usr/bin/python3 -u /opt/diode_receiver/diode_receiver.py
Restart=on-failure
RestartSec=20s

StandardOutput=append:/var/log/diode_receiver.log
StandardError=inherit

[Install]
WantedBy=multi-user.target" | sudo tee /lib/systemd/system/diode-receiver.service

sudo chmod 644 /lib/systemd/system/diode-receiver.service

sudo systemctl daemon-reload
sudo systemctl enable diode-receiver.service
sudo systemctl start diode-receiver
