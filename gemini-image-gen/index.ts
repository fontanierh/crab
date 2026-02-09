import { GoogleGenAI, Modality } from "@google/genai";

const apiKey = process.env.GOOGLE_API_KEY;

if (!apiKey) {
  console.error("Missing GOOGLE_API_KEY environment variable.");
  process.exit(1);
}

const ai = new GoogleGenAI({ apiKey });
const prompt = process.argv.slice(2).join(" ") || "A playful robot juggling oranges in a sunlit kitchen, photorealistic";

const response = await ai.models.generateContent({
  model: "gemini-3-pro-image-preview",
  contents: prompt,
  config: {
    responseModalities: [Modality.IMAGE, Modality.TEXT],
  },
});

let savedImage = false;
for (const part of response.candidates?.[0]?.content?.parts ?? []) {
  if (!part.inlineData?.data) {
    if (part.text) {
      console.log(part.text);
    }
    continue;
  }

  const imageData = part.inlineData.data;
  const bytes = Buffer.from(imageData, "base64");
  const outputPath = "./output.png";
  await Bun.write(outputPath, bytes);
  console.log(`Saved image to ${outputPath}`);
  savedImage = true;
}

if (!savedImage) {
  console.log("No image returned. Try adjusting the prompt.");
}
