This directory stores the Tensorflow model files for DeepMAR.

There should be two files: _DeepMAR_frozen.pb_ and _tf_session_config.pb_.

However, due to the size of _DeepMAR_frozen.pb_, it is not included in Git.

It can be generated from the Caffe model file for DeepMAR.
First, use the [Caffe-Tensorflow converting tool](https://github.com/kyu-sz/caffe-tensorflow/tree/master)
to convert the Caffe model into a Tensorflow model.
Then use the
[model saving example](https://github.com/kyu-sz/caffe-tensorflow/tree/master/examples/save_model)
to save the Tensorflow model into a frozen graph protobuf.

To retrieve the DeepMAR Caffe model, see [models/DeepMARCaffe/README.md](models/DeepMARCaffe/README.md).