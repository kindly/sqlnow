import { useParams } from "react-router-dom";
import QueryForm from "../query-form";

export default function Query() {
  let {query} = useParams();
  return (
    <QueryForm queryType="query" queryName={query} key={query} />
  );
}
