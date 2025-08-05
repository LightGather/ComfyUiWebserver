import websocket 
import uuid
import json
import urllib.request
import urllib.parse

save_image_websocket = 'SaveImageWebsocket'
server_address = "127.0.0.1:8188"
client_id = str(uuid.uuid4())


def get_prompt_with_workflow(input):
    prompt_text = '''
    {
    "6": {
        "inputs": {
        "text": "",
        "clip": [
            "38",
            1
        ]
        },
        "class_type": "CLIPTextEncode",
        "_meta": {
        "title": "CLIP Text Encode (Positive Prompt)"
        }
    },
    "8": {
        "inputs": {
        "samples": [
            "13",
            0
        ],
        "vae": [
            "10",
            0
        ]
        },
        "class_type": "VAEDecode",
        "_meta": {
        "title": "VAE Decode"
        }
    },
    "9": {
        "inputs": {
        "filename_prefix": "ComfyUI",
        "images": [
            "8",
            0
        ]
        },
        "class_type": "SaveImage",
        "_meta": {
        "title": "Save Image"
        }
    },
    "10": {
        "inputs": {
        "vae_name": "ae.safetensors"
        },
        "class_type": "VAELoader",
        "_meta": {
        "title": "Load VAE"
        }
    },
    "11": {
        "inputs": {
        "clip_name1": "t5xxl_fp16.safetensors",
        "clip_name2": "clip_l.safetensors",
        "type": "flux"
        },
        "class_type": "DualCLIPLoader",
        "_meta": {
        "title": "DualCLIPLoader"
        }
    },
    "12": {
        "inputs": {
        "unet_name": "flux_dev.safetensors",
        "weight_dtype": "default"
        },
        "class_type": "UNETLoader",
        "_meta": {
        "title": "Load Diffusion Model"
        }
    },
    "13": {
        "inputs": {
        "noise": [
            "25",
            0
        ],
        "guider": [
            "22",
            0
        ],
        "sampler": [
            "16",
            0
        ],
        "sigmas": [
            "17",
            0
        ],
        "latent_image": [
            "27",
            0
        ]
        },
        "class_type": "SamplerCustomAdvanced",
        "_meta": {
        "title": "SamplerCustomAdvanced"
        }
    },
    "16": {
        "inputs": {
        "sampler_name": "euler"
        },
        "class_type": "KSamplerSelect",
        "_meta": {
        "title": "KSamplerSelect"
        }
    },
    "17": {
        "inputs": {
        "scheduler": "simple",
        "steps": 20,
        "denoise": 1,
        "model": [
            "30",
            0
        ]
        },
        "class_type": "BasicScheduler",
        "_meta": {
        "title": "BasicScheduler"
        }
    },
    "22": {
        "inputs": {
        "model": [
            "38",
            0
        ],
        "conditioning": [
            "26",
            0
        ]
        },
        "class_type": "BasicGuider",
        "_meta": {
        "title": "BasicGuider"
        }
    },
    "25": {
        "inputs": {
        "noise_seed": 1234511
        },
        "class_type": "RandomNoise",
        "_meta": {
        "title": "RandomNoise"
        }
    },
    "26": {
        "inputs": {
        "guidance": 3.5,
        "conditioning": [
            "6",
            0
        ]
        },
        "class_type": "FluxGuidance",
        "_meta": {
        "title": "FluxGuidance"
        }
    },
    "27": {
        "inputs": {
        "width": 1024,
        "height": 1024,
        "batch_size": 1
        },
        "class_type": "EmptySD3LatentImage",
        "_meta": {
        "title": "EmptySD3LatentImage"
        }
    },
    "30": {
        "inputs": {
        "max_shift": 1.15,
        "base_shift": 0.5,
        "width": 1024,
        "height": 1024,
        "model": [
            "12",
            0
        ]
        },
        "class_type": "ModelSamplingFlux",
        "_meta": {
        "title": "ModelSamplingFlux"
        }
    },
    "38": {
        "inputs": {
        "lora_name": "flux_realism_lora.safetensors",
        "strength_model": 1,
        "strength_clip": 1,
        "model": [
            "30",
            0
        ],
        "clip": [
            "11",
            0
        ]
        },
        "class_type": "LoraLoader",
        "_meta": {
        "title": "Load LoRA"
        }
    },
    "40": {
        "inputs": {
        "images": [
            "8",
            0
        ]
        },
        "class_type": "SaveImageWebsocket",
        "_meta": {
        "title": "SaveImageWebsocket"
        }
    }
    }
    '''

    prompt_json = json.loads(prompt_text)
    prompt_json["6"]["inputs"]["text"] = input
    prompt_json["27"]["inputs"]["width"] = 512
    prompt_json["27"]["inputs"]["height"] = 512
    prompt_json["30"]["inputs"]["width"] = 512
    prompt_json["30"]["inputs"]["height"] = 512
    return prompt_json

def queue_prompt(prompt):
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode('utf-8')
    req =  urllib.request.Request("http://{}/prompt".format(server_address), data=data)
    return json.loads(urllib.request.urlopen(req).read())

def get_image(filename, subfolder, folder_type):
    data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
    url_values = urllib.parse.urlencode(data)
    with urllib.request.urlopen("http://{}/view?{}".format(server_address, url_values)) as response:
        return response.read()

def get_history(prompt_id):
    with urllib.request.urlopen("http://{}/history/{}".format(server_address, prompt_id)) as response:
        return json.loads(response.read())

def get_images(ws, prompt):
    prompt_id = queue_prompt(prompt)['prompt_id']
    output_image = None
    current_node = ""

    while True:
        out = ws.recv()
        if isinstance(out, str):
            message = json.loads(out)
            if message['type'] == 'executing':
                data = message['data']
                if data['prompt_id'] == prompt_id:
                    if data['node'] is None:
                        print("Execution is done")
                        break
                    else:
                        node_number = data['node']
                        current_node = prompt[node_number]["class_type"]
                        print(f"Executing node: {current_node}") # 현재 실행 중인 노드 출력

            elif message['type'] == 'progress': # 이 부분이 추가됨
                data = message['data']
                progress_percentage = (data['value'] / data['max']) * 100
                print(f"Progress: {progress_percentage:.1f}% ({data['value']}/{data['max']})")
        else:
            if current_node == save_image_websocket:
                output_image = out[8:]

    return output_image

def fetch_image_from_comfy(input):
    ws = websocket.WebSocket()
    ws.connect("ws://{}/ws?clientId={}".format(server_address, client_id))
    images = get_images(ws, get_prompt_with_workflow(input))
    ws.close()
    return images
