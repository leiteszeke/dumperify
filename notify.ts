import * as nodemailer from "nodemailer";
import * as fs from "node:fs";

type NotifyConfig = {
  smtp: {
    host: string;
    port: number;
    secure: boolean;
    user: string;
    password: string;
  };
  from: string;
  to: string[];
};

let config: NotifyConfig | null = null;

const loadConfig = (): NotifyConfig | null => {
  if (config) return config;

  try {
    const raw = fs.readFileSync("./notify.json", "utf-8");
    config = JSON.parse(raw) as NotifyConfig;
    return config;
  } catch {
    console.warn(
      "Notification config (notify.json) not found. Notifications disabled.",
    );
    return null;
  }
};

export const sendAlert = async (
  subject: string,
  message: string,
): Promise<void> => {
  const cfg = loadConfig();

  if (!cfg) return;

  const transporter = nodemailer.createTransport({
    host: cfg.smtp.host,
    port: cfg.smtp.port,
    secure: cfg.smtp.secure,
    auth: {
      user: cfg.smtp.user,
      pass: cfg.smtp.password,
    },
  });

  try {
    await transporter.sendMail({
      from: cfg.from,
      to: cfg.to.join(", "),
      subject: `[Dumperify] ${subject}`,
      text: message,
    });

    console.log(`Alert email sent: ${subject}`);
  } catch (error) {
    console.error("Failed to send alert email:", error);
  }
};
