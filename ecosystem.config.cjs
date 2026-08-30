module.exports = {
  apps: [
    {
      name: "twitter-likes-organizer-4024",
      script: ".venv/bin/uvicorn",
      args: "src.server.app:app --host 0.0.0.0 --port 4024",
      cwd: __dirname,
      interpreter: "none",
      env_file: ".env",
      env: {
        HOST: "0.0.0.0",
        PORT: 4024,
      },
      max_restarts: 10,
      restart_delay: 2000,
      autorestart: true,
    },
  ],
};
