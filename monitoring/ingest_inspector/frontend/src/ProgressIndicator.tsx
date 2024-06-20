import { useEffect } from "react";
import { Router } from "react-router-dom";
import NProgress from "nprogress";

const LoadingIndicator = () => {
  function handleRouteChangeStart() {
    NProgress.start();
  }

  function handleRouteChangeComplete() {
    NProgress.done();
  }

  // useEffect(() => {
  //     Router.events.on('routeChangeStart', handleRouteChangeStart);
  //     Router.events.on('routeChangeComplete', handleRouteChangeComplete);
  //
  //     return () => {
  //         Router.events.off('routeChangeStart', handleRouteChangeStart);
  //         Router.events.off('routeChangeComplete', handleRouteChangeComplete);
  //     };
  // }, []);

  NProgress.configure({
    showSpinner: false,
    parent: ".loading-indicator-wrapper",
  });

  return <div className="loading-indicator-wrapper" />;
};
