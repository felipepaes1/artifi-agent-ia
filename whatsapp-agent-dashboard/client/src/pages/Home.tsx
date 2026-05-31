// Redirect to Dashboard - this file is kept for compatibility
import { Redirect } from "wouter";

export default function Home() {
  return <Redirect to="/" />;
}
