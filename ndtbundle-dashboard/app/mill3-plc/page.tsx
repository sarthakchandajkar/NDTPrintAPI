import { redirect } from "next/navigation";

/** @deprecated Use /mills-plc */
export default function Mill3PlcRedirect() {
  redirect("/mills-plc");
}
