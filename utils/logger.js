"use strict";

const colors = {
  Reset: "\x1b[0m",
  Bright: "\x1b[1m",
  Dim: "\x1b[2m",
  Underscore: "\x1b[4m",
  Blink: "\x1b[5m",
  Reverse: "\x1b[7m",
  Hidden: "\x1b[8m",
  fg: {
    Black: "\x1b[30m",
    Red: "\x1b[31m",
    Green: "\x1b[32m",
    Yellow: "\x1b[33m",
    Blue: "\x1b[34m",
    Magenta: "\x1b[35m",
    Cyan: "\x1b[36m",
    White: "\x1b[37m",
    Crimson: "\x1b[38m" //القرمزي
  },
  bg: {
    Black: "\x1b[40m",
    Red: "\x1b[41m",
    Green: "\x1b[42m",
    Yellow: "\x1b[43m",
    Blue: "\x1b[44m",
    Magenta: "\x1b[45m",
    Cyan: "\x1b[46m",
    White: "\x1b[47m",
    Crimson: "\x1b[48m"
  }
};

function timestamp() {
  return new Date().toLocaleString("en", {
    timeZoneName: "short",
    timeZone: "Asia/Ho_Chi_Minh"
  });
}
class logger {
  static error(message) {
    console.log(
      colors.fg.Red,
      `[${timestamp()}] [ERROR] ${message || ""}`,
      colors.Reset
    );
  }
  static success(message) {
    console.log(
      colors.fg.Green,
      `[${timestamp()}] [SUCCESS] ${message || ""}`,
      colors.Reset
    );
  }
  static warn(message) {
    console.log(
      colors.fg.Yellow,
      `[${timestamp()}] [WARN] ${message || ""}`,
      colors.Reset
    );
  }
  static info(message) {
    console.log(
      colors.fg.Blue,
      `[${timestamp()}] [INFO] ${message || ""}`,
      colors.Reset
    );
  }
}

module.exports = logger;
