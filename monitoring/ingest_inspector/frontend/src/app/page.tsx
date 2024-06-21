'use client';

import RecentIngests from "./components/RecentIngests";
import Form from "./components/Form";

const HomePage = () => {
  return (
      <div>
        <div
          className="content"
          style={{ marginBottom: "2em", marginTop: "1em" }}
        >
          <Form/>
        </div>  
        <hr />
        <RecentIngests />
      </div>
  );
};

export default HomePage;
