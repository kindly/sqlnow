import { useRouteError, Link } from "react-router-dom";

export default function ErrorPage() {
  const error = useRouteError();
  console.error(error);

  return (
    <div className="flex h-full items-center justify-center bg-bg font-sans text-ink">
      <div className="text-center">
        <p className="font-mono text-sm font-medium">Something went wrong</p>
        <p className="mt-2 font-mono text-xs text-danger">
          {error.statusText || error.message}
        </p>
        <Link to="/" className="mt-4 inline-block font-mono text-xs text-accent hover:underline">
          Back to sqlnow
        </Link>
      </div>
    </div>
  );
}
