# digitaltwindenboschbackend
digitaltwindenboschbackend


# You need DOCKERSETUP first and for Deployment Instructions

1.  Clone this repository to your local server.
2.  Install Docker and Docker Compose on your local server.
3.  Navigate to the repository directory.
4.  Run `docker-compose up --build -d` to build and run the containers.
5.  Access the application at `http://your-server-ip:your-port`.

# Updating the Server

1.  Push your local changes to GitHub.
2.  SSH into your local server.
3.  Navigate to the repository directory.
4.  Run `git pull`.
5.  Run `docker-compose down` then `docker-compose up --build -d` to rebuild and restart the containers.
