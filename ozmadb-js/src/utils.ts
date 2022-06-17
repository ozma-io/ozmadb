/*
 * Helpful functions.
 */

export const goodName = (name: string): boolean => {
  return !(name === "" || name.includes(' ') || name.includes('/') || name.includes("__"));
}
