import type { Metadata } from "next";
import "./globals.css";
import Link from "next/link";
import { ReactNode, Suspense } from "react";

const GITHUB_REPO_URL = "https://github.com/wellcomecollection/storage-service";

export const metadata: Metadata = {
  title: "Ingest Inspector",
};

type RootLayoutProps = {
  children: ReactNode;
};

export default function RootLayout({ children }: RootLayoutProps) {
  return (
    <html lang="en">
      <body>
        <header>
          <div className="loading-indicator-wrapper w-full h-1 top-0 absolute" />
          <div className="content">
            <Link href="/?" className="no-underline hover:underline">
              wellcome ingest inspector
            </Link>
          </div>
        </header>
        <main>
          <Suspense>{children}</Suspense>
        </main>
        <footer className="w-full">
          <div className="content mt-6">
            <p>
              made with <span className="heart">♥</span> • source on{" "}
              <a href={GITHUB_REPO_URL} target="_blank" rel="noreferrer">
                GitHub
              </a>
            </p>
          </div>
        </footer>
      </body>
    </html>
  );
}
