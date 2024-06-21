'use client';

import RecentIngests from "./components/RecentIngests";
import Form from "./components/Form";

const HomePage = () => {
  return (
      <div>
        <Form/>
        <hr />
        <RecentIngests />
      </div>
  );
};

export default HomePage;
