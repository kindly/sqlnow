import { useParams } from "react-router-dom";
import QueryForm from "../query-form";

export default function Table() {
  let { table } = useParams();
  return (
    <QueryForm queryType="table" queryName={table} key={"table-" + table} />
  );
}