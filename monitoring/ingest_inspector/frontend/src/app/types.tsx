export type IngestData = {
  id: string;
  bag: {
    info: {
      externalIdentifier: string;
      version: string;
      type: string;
    };
    type: string;
  };
  createdDate: string;
  ingestType: {
    id: string;
    type: string;
  };
  s3Url: string;
  displayS3Url: string;
  lastUpdatedDate: string;
  _repeated: boolean;
  _count: number;
  sourceLocation: {
    bucket: string;
    path: string;
    provider: {
      id: string;
      type: string;
    };
    type: string;
  };
  space: {
    id: string;
    type: string;
  };
  status: {
    id: string;
    type: string;
  };
  type: string;
  callback?: {
    status: {
      id: string;
      type: string;
    };
  };
  events: {
    kibanaUrl: string;
    _is_unmatched_start: boolean;
    createdDate: string;
    description: string;
    _count: number;
    _repeated: boolean;
  }[];
  environment: string;
};
