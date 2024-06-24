import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Link from "next/link";
import cx from "classnames";
import {Suspense} from "react";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Ingest Inspector"
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {

  return (
    <html lang="en">
      <body className={inter.className}>
      <header>
        <div className="loading-indicator-wrapper w-full h-1 top-0 absolute" />
        <div className="content">
          <Link href="/?">wellcome ingest inspector</Link>
        </div>
      </header>
      <main>
          <Suspense>
        {children}
          </Suspense>
      </main>
      <footer className="w-full">
        <div className="content mt-6">
          <p>
            made with{" "}
            <span className="heart">
              ♥
            </span>{" "}
            • source on{" "}
            <a href="https://github.com/wellcomecollection/storage-service" target="_blank" rel="noreferrer">
              GitHub
            </a>
          </p>
        </div>
      </footer>        
      </body>
    </html>
  );
}
