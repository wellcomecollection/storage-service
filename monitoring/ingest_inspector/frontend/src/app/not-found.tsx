"use client";

import { useRouter } from "next/navigation";
import { useEffect } from "react";

const NotFoundPage = () => {
  const router = useRouter();

  // Redirect user to home page
  useEffect(() => {
    router.replace("/");
  });
};

export default NotFoundPage;
