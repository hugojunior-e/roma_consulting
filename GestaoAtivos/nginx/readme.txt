/etc/systemd/system/algaretl.service


[Unit]
Description=AlgarETL instance for my Flask app
After=network.target

[Service]
User=producao
Group=producao
WorkingDirectory=/app
ExecStart=gunicorn --workers 3 --bind 127.0.0.1:8000 app:app

[Install]
WantedBy=multi-user.target






sudo systemctl daemon-reload
sudo systemctl start algaretl
sudo systemctl enable algaretl
















/etc/nginx/sites-available/algaretl


server {
    listen 80;
    server_name 10.11.136.52;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}


sudo ln -s /etc/nginx/sites-available/algaretl /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx














criar o algaretl.conf em /etc/nginx/conf.d/
criar o algaretl.service em /etc/systemd/system/


sudo systemctl restart nginx


sudo systemctl daemon-reload
sudo systemctl enable algaretl
sudo systemctl start algaretl
