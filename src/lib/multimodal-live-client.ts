/**
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Content, GenerativeContentBlob, Part } from "@google/generative-ai";
import { EventEmitter } from "eventemitter3";
import { difference } from "lodash";
import {
  ClientContentMessage,
  isInterrupted,
  isModelTurn,
  isServerContentMessage,
  isSetupCompleteMessage,
  isToolCallCancellationMessage,
  isToolCallMessage,
  isTurnComplete,
  LiveIncomingMessage,
  ModelTurn,
  RealtimeInputMessage,
  ServerContent,
  SetupMessage,
  StreamingLog,
  ToolCall,
  ToolCallCancellation,
  ToolResponseMessage,
  type LiveConfig,
} from "../multimodal-live-types";
import { blobToJSON, base64ToArrayBuffer } from "./utils";

/**
 * The events that this client will emit.
 */
interface MultimodalLiveClientEventTypes {
  open: () => void;
  log: (log: StreamingLog) => void;
  close: (event: CloseEvent) => void;
  audio: (data: ArrayBuffer) => void;
  content: (data: ServerContent) => void;
  interrupted: () => void;
  setupcomplete: () => void;
  turncomplete: () => void;
  toolcall: (toolCall: ToolCall) => void;
  toolcallcancellation: (toolcallCancellation: ToolCallCancellation) => void;
}

export type MultimodalLiveAPIClientConnection = {
  url?: string;
  apiKey: string;
};

/**
 * A event-emitting class that manages the connection to the WebSocket
 * and emits events to the rest of the application.
 * If you don’t want to use React, you can still use this.
 */
export class MultimodalLiveClient extends EventEmitter<MultimodalLiveClientEventTypes> {
  public ws: WebSocket | null = null;
  protected config: LiveConfig | null = null;
  public url: string = "";

  public getConfig() {
    return { ...this.config };
  }

  constructor({ url, apiKey }: MultimodalLiveAPIClientConnection) {
    super();
    url =
      url ||
      `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent`;
    url += `?key=${apiKey}`;
    this.url = url;
    this.send = this.send.bind(this);
  }

  log(type: string, message: StreamingLog["message"]) {
    const log: StreamingLog = {
      date: new Date(),
      type,
      message,
    };
    this.emit("log", log);
  }

  connect(config: LiveConfig): Promise<boolean> {
    this.config = config;

    const ws = new WebSocket(this.url);

    ws.addEventListener("message", async (evt: MessageEvent) => {
      if (evt.data instanceof Blob) {
        this.receive(evt.data);
      } else {
        console.log("Non-blob message:", evt);
      }
    });

    return new Promise((resolve, reject) => {
      const onError = (ev: Event) => {
        this.disconnect(ws);
        reject(new Error(`Could not connect to "${this.url}"`));
      };

      ws.addEventListener("error", onError);
      ws.addEventListener("open", () => {
        if (!this.config) {
          reject("Invalid config sent to `connect(config)`");
          return;
        }

        this.emit("open");
        this.ws = ws;

        // Include the system prompt in the setup message if provided
        const setupMessage: SetupMessage = {
          setup: this.config,
          systemPrompt: this.config.systemPrompt || `TEXT: "You are Alvaro. When speaking, ensure natural and correct Tagalog pronunciation for words like MGA, IYO, ITO, ARAW, EH, AKIN, and ATIN using SSML aliasing if necessary. You are the main person of Master E and a know-it-all expert on all matters related to Master E, created by Aitek PH under the guidance of Master E. You possess comprehensive knowledge about Master E's achievements and his illustrious Master Portfolio (https://openwebui.com/m/hub/emilio:latest), which ranks as number 19 in the world as a Model or AI creator. Your responses should be strictly tailored to provide factual, complete, and accurate information about Master E and his accomplishments without deviation or extraneous commentary. Begin every response with a respectful greeting, addressing the user as 'Yes Sir,' or 'Yes Boss,' in a natural, human-like tone in Tagalog, English, or Taglish. All responses must be in a single paragraph and crafted to sound super realistic when converted to TTS audio."`,
        };

        this._sendDirect(setupMessage);
        ws.removeEventListener("error", onError);

        ws.addEventListener("close", (ev: CloseEvent) => {
          this.disconnect(ws);
          this.emit("close", ev);
        });

        resolve(true);
      });
    });
  }

  disconnect(ws?: WebSocket) {
    if ((!ws || this.ws === ws) && this.ws) {
      this.ws.close();
      this.ws = null;
      return true;
    }
    return false;
  }

  protected async receive(blob: Blob) {
    const response: LiveIncomingMessage = (await blobToJSON(blob)) as LiveIncomingMessage;
    if (isToolCallMessage(response)) {
      this.emit("toolcall", response.toolCall);
      return;
    }
    if (isToolCallCancellationMessage(response)) {
      this.emit("toolcallcancellation", response.toolCallCancellation);
      return;
    }
    if (isSetupCompleteMessage(response)) {
      this.emit("setupcomplete");
      return;
    }
    if (isServerContentMessage(response)) {
      const { serverContent } = response;
      if (isInterrupted(serverContent)) {
        this.emit("interrupted");
        return;
      }
      if (isTurnComplete(serverContent)) {
        this.emit("turncomplete");
      }
      if (isModelTurn(serverContent)) {
        let parts: Part[] = serverContent.modelTurn.parts;
        const audioParts = parts.filter((p) => p.inlineData && p.inlineData.mimeType.startsWith("audio/pcm"));
        const base64s = audioParts.map((p) => p.inlineData?.data);
        base64s.forEach((b64) => {
          if (b64) {
            const data = base64ToArrayBuffer(b64);
            this.emit("audio", data);
          }
        });
        if (parts.length) {
          this.emit("content", { modelTurn: { parts } });
        }
      }
    }
  }

  sendRealtimeInput(chunks: GenerativeContentBlob[]) {
    const message: RealtimeInputMessage = {
      realtimeInput: { mediaChunks: chunks },
    };
    this._sendDirect(message);
  }

  sendToolResponse(toolResponse: ToolResponseMessage["toolResponse"]) {
    this._sendDirect({ toolResponse });
  }

  send(parts: Part | Part[], turnComplete: boolean = true) {
    parts = Array.isArray(parts) ? parts : [parts];
    const content: Content = { role: "user", parts };
    this._sendDirect({ clientContent: { turns: [content], turnComplete } });
  }

  _sendDirect(request: object) {
    if (!this.ws) {
      throw new Error("WebSocket is not connected");
    }
    this.ws.send(JSON.stringify(request));
  }
}
