/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        wl: {
          50: "#f2fbfb",
          100: "#ddf4f3",
          200: "#b9e9e8",
          300: "#87d8d7",
          400: "#4fc1c0",
          500: "#27a5a4",
          600: "#168687",
          700: "#136a6c",
          800: "#145557",
          900: "#15484a",
        },
      },
      boxShadow: {
        panel: "0 12px 30px rgba(15, 23, 42, 0.10)",
        soft: "0 6px 18px rgba(15, 23, 42, 0.08)",
      },
      fontFamily: {
        sans: ["Plus Jakarta Sans", "Noto Sans SC", "sans-serif"],
      },
    },
  },
  plugins: [],
}
