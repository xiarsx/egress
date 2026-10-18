export function parseLinesToList(input: string | undefined, normalize?: (value: string) => string): string[] {
  if (!input) {
    return [];
  }

  return input
    .split(/\n/)
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => (normalize ? normalize(item) : item));
}
