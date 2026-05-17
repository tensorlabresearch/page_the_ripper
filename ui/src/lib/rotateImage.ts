/**
 * Render `src` rotated by `degrees` clockwise into a JPEG data URL.
 * The resulting image's natural dimensions match the rotated bounds, so
 * react-image-crop will report crop coordinates in *rotated* image space —
 * which the backend interprets correctly when it passes the same rotation
 * to PIL.Image.rotate before cropping.
 *
 * Returns the original src unchanged when degrees is 0 (mod 360).
 */
export async function rotateImageToDataUrl(src: string, degrees: number): Promise<string> {
  const angle = ((degrees % 360) + 360) % 360;
  if (angle === 0) return src;

  const img = await loadImage(src);
  const rad = (angle * Math.PI) / 180;
  const sin = Math.abs(Math.sin(rad));
  const cos = Math.abs(Math.cos(rad));
  const w = img.naturalWidth;
  const h = img.naturalHeight;
  const canvas = document.createElement('canvas');
  canvas.width = Math.round(w * cos + h * sin);
  canvas.height = Math.round(w * sin + h * cos);
  const ctx = canvas.getContext('2d');
  if (!ctx) throw new Error('canvas 2d context unavailable');
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.translate(canvas.width / 2, canvas.height / 2);
  ctx.rotate(rad);
  ctx.drawImage(img, -w / 2, -h / 2);
  return canvas.toDataURL('image/jpeg', 0.85);
}

function loadImage(src: string): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error(`failed to load image: ${src}`));
    img.src = src;
  });
}
